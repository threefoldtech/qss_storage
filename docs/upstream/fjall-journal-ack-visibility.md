# Journal write visibility vs the ack: an fjall 3.1.8 field report

**Audience**: fjall-rs maintainers (eventually), and this repo's own record.
**Status**: tech doc only -- deliberately NOT yet an issue or PR upstream.
One finding here is confirmed-and-fixed on our side; the crash-loss
mechanism itself is OPEN, with a preserved corpse that constrains it.
**Date**: 2026-08-01 (revised same day: the first draft's mechanism
hypothesis did not survive its own regression test, and this document says
so rather than pretending otherwise)
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
next consumer the archaeology. OPEN: the kill -9 losses themselves are NOT
explained by the userspace journal buffer, which was our first theory --
the corpse of a reproduced loss (store snapshotted between kill and
restart, before recovery) shows acknowledged records absent from the
on-disk journal in a pattern that theory cannot produce, detailed below.
We would value a maintainer's read on it.

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
at every durability level. That test killed our first theory (below) and
now pins the boundary permanently in our suite.

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

## Finding 2 (open): the kill -9 losses, and the corpse that refuses both theories

The observations:

- Three acknowledged CompleteMultipartUpload results lost across kill -9
  at our buffer level, out of ~45 hot-storm crash cycles over two days;
  never a plain PUT, never at our fsync level, and 492 kill-at-the-ack
  attempts reproduce nothing.
- The reproduction we finally captured (a `cp -a` of the store between the
  kill and the restart, so recovery could not touch the tail): the lost
  key appears exactly TWICE in the on-disk journals -- its
  create-multipart-upload records -- while each surviving neighbor key
  (acknowledged seconds earlier and seconds later) appears ~15 times. The
  lost key's part-upload records and its object record are absent from
  every `.jnl` file in the snapshot. No journal rotation had occurred.

Why our first theory died: "the record was still in the 8 KiB userspace
BufWriter when the kill landed" explains a missing TAIL. It does not
explain this shape -- the part-upload acks happened SECONDS before the
kill, on the busy database whose buffer rolls constantly, and (per Finding
1's investigation) every one of those writes ends in a
`persist(PersistMode::Buffer)` that drains the buffer to the kernel. Once
write() returns, `kill -9` cannot unwrite it. Writes acknowledged AFTER
the lost key's survived the same kill. An append-only, single-writer,
mutex-serialized journal should not be able to contain later entries while
physically missing earlier flushed ones.

Constraints any explanation has to satisfy: process kill only (no power
involved); the missing records span two databases (parts in one, the
object record in the other); the corpse's journal files carry the 64 MiB
`set_len` preallocation with the written region ending before the missing
records; recovery afterwards behaved correctly for what the files
contained. Things this suggests to us, none verified: something in the
journal manager / memtable-seal path that can drop or redirect buffered
entries under concurrency; or a write path for these specific operations
that does not end in the persist we think it does; or an error swallowed
somewhere that poisoned less than it should have. We know what it is NOT:
rotation (`Writer::rotate` begins with `persist(SyncAll)` under the writer
lock) and tx write-behind (batch commit `write_batch`es synchronously
under the same lock) were both cleared by source reading, and the plain
BufWriter-tail theory is refuted above.

Upstream suggestion 2 (really a question): does this shape ring a bell? We
can share the corpse (pre-recovery journals + sstables + the acknowledged
set, 4.8 MiB) and the harness that reproduced it (~1 loss per 15 hot-storm
crash cycles).

## Reproduction, if wanted

All in the qss_storage repo (github.com/threefoldtech/qss_storage):

- `tests/real/tools/buffer-loss-repro.sh` -- the kill-at-ack rig; its
  hundreds of clean kills are themselves part of the evidence.
- `tests/real/run.sh --fresh --phase 7` with `QSSRT_CRASH_CYCLES=15` and
  `QSSRT_CRASH_SNAPSHOT_DIR=<dir>` -- the hot-storm crash matrix with
  pre-recovery snapshots; reproduced the loss 1-in-15 cycles on xfs.
- The corpse: preserved outside the repo, available on request.
- The permanent regression tests for the write-visibility boundary:
  `cas-storage/src/metastore/stores/fjall.rs` (journal-bytes assertions)
  and `cas-storage/src/cas/ack_visibility_tests.rs`.
