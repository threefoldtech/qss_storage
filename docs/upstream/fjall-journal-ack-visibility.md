# Journal write visibility vs the ack: an fjall 3.1.8 field report

**Audience**: fjall-rs maintainers (eventually), and this repo's own record.
**Status**: tech doc only -- deliberately NOT yet an issue or PR upstream.
**Date**: 2026-08-01
**fjall**: 3.1.8 (crates.io; source cross-checked against the fjall-rs/fjall
repo at tag 3.1.8, local clone at ~/prppl/fjall)
**Consumer**: qss_storage (content-addressed S3 store; fjall via
`SingleWriterTxDatabase`, one database for block records, one per-namespace
database for object records)

---

## One-paragraph summary

An application that acknowledges writes to its clients after
`keyspace.insert()` returns -- without calling `persist()` -- has
acknowledged data that may not exist ANYWHERE outside its own process
memory: not on disk, not in the page cache, invisible to the kernel. fjall's
journal writer buffers entries in an 8 KiB userspace `BufWriter`, nothing
bounds how long an entry may sit there, and `PersistMode::Buffer` -- whose
name suggests "the cheapest tier, basically what happens anyway" -- is in
fact the operation that would have saved the data (it drains the buffer to
the OS). We hit this in production-grade crash testing, lost acknowledged
records across `kill -9`, spent a night suspecting journal rotation and
write-behind (both innocent), and only closed it with a post-kill snapshot
of the store taken before recovery ran. This document is the write-up we
wish we could have read first: the mechanism, the evidence, the consumer
mistake, and two small upstream suggestions that would make the trap harder
to fall into. The primary fault is ours -- we did not call `persist()` on an
ack-carrying path. The sharp edge is real nonetheless.

---

## The mechanism, from source (fjall 3.1.8)

`src/journal/writer.rs`:

- The journal writer wraps its file in a `BufWriter` with
  `JOURNAL_BUFFER_BYTES = 8 * 1024` (writer.rs:21, :177, :194).
- `write_raw` (single op) and `write_batch` (batch/tx commit) serialize
  entries and `write_all` them INTO THE BUFWRITER, setting
  `is_buffer_dirty = true` (writer.rs:258-298, :326-379). No flush happens
  here. When these return, the bytes are in process memory only.
- `Writer::persist(mode)` (writer.rs:203-234) first drains the BufWriter to
  the kernel (`self.file.flush()`) whenever it is dirty, and THEN applies
  the mode: `SyncAll` -> `sync_all()`, `SyncData` -> `sync_data()`,
  `Buffer` -> `Ok(())`.

So the three `PersistMode`s are not "nothing / fdatasync / fsync". They are
"write() / write()+fdatasync / write()+fsync". `PersistMode::Buffer` is not
a no-op tier -- it is the write() tier, and it only exists for callers who
CALL persist. A consumer who never calls `persist()` gets a fourth,
undocumented tier: nothing at all, bounded only by the 8 KiB buffer rolling
over from later traffic (or journal rotation's internal
`persist(SyncAll)`, writer.rs:67, at the default 64 MiB pre-allocation).

There is no time bound. On a quiet keyspace, an entry can sit in the
userspace buffer indefinitely.

## What we observed

Contract under test: our `buffer` durability level promises "acknowledged
writes survive process kill; only power loss / OS crash may take them" --
i.e. write()-before-ack semantics. Our crash campaign (kill -9 mid-storm,
restart, verify every client-acknowledged object) produced:

- Hundreds of storm PUTs per cycle: acknowledged, survived, every cycle.
- THREE lost objects across ~45 buffer-durability cycles over two days
  (`mp-94`, `mp-61`, then reproduced-on-purpose `mp-68`) -- every single
  one a multipart-complete acknowledgement, never a storm PUT.
- A targeted rig doing kill-the-instant-complete-acks: 492 kills across
  btrfs and xfs, ZERO losses. The naive window does not reproduce it.

The decisive instrument was a store snapshot (`cp -a`) taken between the
kill and the restart, so recovery could not rewrite the journal tail before
we read it. In the reproduction's corpse
(`target/realtest/loss-snapshots-20260801T144750/buffer-cycle7`):

- The lost key's multipart and part records ARE present in the (busy,
  frequently-rolled) blocks-database journal.
- The lost key's object record -- the write its acknowledgement rode on --
  appears NOWHERE in the namespace-database journal file, while its
  immediate neighbors (`mp-66`, `mp-67`, acknowledged seconds earlier) are
  present and survived.

That pattern is exactly the 8 KiB buffer: the namespace database sees one
small record per object (thin traffic, buffer lingers), the blocks
database sees a flood (buffer rolls constantly). The record died in
userspace, pre-write(); recovery then correctly replayed a journal that
genuinely never contained it. It also explains the 492-kill negative: a
kill immediately after a quiet ack usually lands just after the buffer
happened to roll; only a hot storm keeps the tail wide.

## The consumer-side fault (ours)

Our transactional writes were correct all along: every transaction commit
is followed by `db.persist(<configured mode>)`. What was not correct: our
non-transactional tree writes (`keyspace.insert()` directly) -- which is
the path our object records take, i.e. the very write each S3
acknowledgement stands on -- called `persist()` NEVER, at ANY durability
level. Two consequences:

- At our `buffer` level: the proven kill-loss above.
- At our `fsync` level: a latent POWER-LOSS window. The object record
  reaches the kernel when the 8 KiB buffer rolls and stable storage only
  at journal rotation. Our campaign is kill-only by design (no power-loss
  rig yet), which is why this second gap produced no finding; the source
  makes it unarguable.

Fix (in progress on our side, rides with our group-commit change):
ack-carrying non-transactional writes are followed by `persist()` at the
store's durability before the acknowledgement -- `PersistMode::Buffer` at
our buffer level (a write(), near-free), `SyncAll` at our fsync level (a
real added fsync; correctness first). Not fjall's bug to fix.

## Why we are writing to you anyway: two suggestions

1. **Documentation sharpening.** `PersistMode::Buffer`'s doc comment
   ("Flushes data to OS buffers...") is accurate for CALLERS OF PERSIST,
   but the enum reads like a durability ladder whose bottom rung is "what
   you get anyway". The trap is believing insert-then-ack already has
   Buffer semantics. One sentence on `insert`/`remove`/`Batch::commit` --
   "data is buffered in process memory until `persist()` is called or the
   journal buffer fills; an application that acknowledges writes without
   `persist(PersistMode::Buffer)` can lose acknowledged data on process
   crash" -- would likely have saved us (and, we suspect, others: this
   failure needs a hot process, a kill, and a quiet keyspace to show
   itself, and it hides from naive crash tests, which is the worst kind of
   rare).

2. **An opt-in bound on buffer residence.** Either a keyspace/database
   option to flush-on-commit (write() only -- the cost is a syscall per
   commit, and applications that want fewer can group), or a time bound (a
   flush of dirty journal buffers on some small interval). Either would
   convert "unbounded invisible window on quiet keyspaces" into a bounded
   one for consumers who never learned to call persist. We are NOT asking
   for a behavior change to defaults; the current design is coherent and
   fast, and everything needed already exists via `persist()`.

## Reproduction, if wanted

All in the qss_storage repo (github.com/threefoldtech/qss_storage):

- `tests/real/tools/buffer-loss-repro.sh` -- the naive kill-at-ack rig
  (expected result: no loss; that negative is part of the story).
- `tests/real/run.sh --fresh --phase 7` with `QSSRT_CRASH_CYCLES=15` and
  `QSSRT_CRASH_SNAPSHOT_DIR=<dir>` -- the hot-storm crash matrix with
  pre-recovery snapshots; reproduced the loss 1-in-15 cycles on xfs.
- The corpse itself (pre-recovery journal + sstables + the acknowledged
  set, 4.8 MiB) is preserved and can be shared on request.

## Non-conclusions

For completeness, two mechanisms we suspected first and cleared by source
reading, so nobody re-suspects them: journal rotation
(`Writer::rotate` begins with `persist(SyncAll)` under the writer lock --
no window there) and tx write-behind (batch commit `write_batch`es
synchronously under the same lock -- ordering is fine). The buffer between
`write_all` and `flush` is the whole story.
