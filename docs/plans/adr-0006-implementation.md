# ADR 0006 Implementation Plan

**Status**: EXECUTED 2026-07-31. All nine components landed on
`development` as `4b0eaca..8893cf5`, one commit per component, each
gate-green (fmt, clippy -D warnings, full test suite). Kept as the
record of what was built and why in this order.

**Implements**: `docs/adr/0006-block-write-protocol.md` as decided
2026-07-31 (file-first block writes, per-block striped locking,
full-id adaptive-depth paths). The ADR is the authority on *why*; this
plan is
the component-level *what*. Written to be executed in a fresh session
with no other context.

**Decisions already made -- do not reopen** (rationale in the ADR):
- Path scheme (owner's, confirmed 2026-07-31): full-id filenames at
  adaptive fanout depth -- `blocks/<b0>/../<b(d-1)>/<full-hex id>`,
  any depth (bounded only by the id width) chosen by placement policy
  at write time; the record stores `d`. A file's name identifies its
  block, so any depth is correct. `_PATHS` and the allocator are
  removed. No migration exists or is needed (no deployed store carries
  data).
- `key_has_block` skip: dropped. Every dedup hit bumps rc under the
  stripe.
- `fjall_notx`: REMOVED outright (ADR 0007, Accepted 2026-07-31). The
  removal lands before this plan executes, so no component carries a
  notx arm; the config value is rejected with a migration message.
- Temp mechanism: named temps in `blocks/.tmp/`, `O_CREAT|O_EXCL`,
  mandatory per-attempt nonce. O_TMPFILE rejected.
- Orphan heal: rename-over unconditionally, never compare-and-skip.
- Durability gating: `Buffer` skips all file/dir fsyncs; `Fsync`/
  `Fdatasync` sync files and directories (directories always via full
  fsync).
- All review asks are resolved. Landing order: 0006 lands before 0005.

**Hard rules carried from the review** (violating any of these
reintroduces a verified loss bug):
1. Every `_BLOCKS` record mutation (insert, bump, decrement, remove)
   happens under `stripe(hash)`.
2. The record-existence check and the rc mutation are one transactional
   read-modify-write. Never a pre-tx read with a blind write.
3. The fjall writer guard lives only inside awaitless synchronous
   stretches (memory-safety: the manual `Send` on `FjallTransaction`,
   `cas-storage/src/metastore/stores/fjall.rs:222-252`).
4. A stripe guard held by a cancellable future must never span an await
   whose detached continuation performs a destructive op. Renames are
   exempt (constructive, idempotent); unlinks are not. Concretely: the
   delete-side re-check + decrement + unlink run inside a
   `spawn_blocking` closure that owns the stripe guard
   (`Arc<Mutex>::lock_owned`).
5. A block record is committed only after its file is durable at its
   final path (crash-true at Fsync/Fdatasync; ordering-true at Buffer).
6. No task acquires a stripe while holding the fjall guard. Fjall is
   the leaf lock.

**Verification gates**: `make fmt` at every commit (pre-commit hook
enforces fmt); `make clippy-check` and `make test` green before any
push (pre-push hook enforces). Run `make test` first for a baseline
before touching anything.

---

## Component 1: Prerequisites (land first, independently)

Two standalone fixes the redesign depends on.

**1a. Commit error mapping.** `write_path.rs:157` and `:167` call
`Box::new(store_tx).commit().unwrap()`. A fjall `PersistError` panics
the connection task and (on the new-block path) leaves a dangling
record with cleanup skipped. Replace both with error mapping into the
closure's existing error channel (`tx.unbounded_send(Err(...))`, as the
surrounding failure arms already do), resolving the guard with
`.failed()`. The new protocol adds more commit sites; none may unwrap.

**1b. buffered_byte_stream comparison bug.**
`cas-storage/src/cas/buffered_byte_stream.rs:58` compares
`self.buffer.len() == buf_remainder` where the incoming `bytes.len()`
is meant; a single stream item >= 512 KiB arriving at an exactly
half-full buffer emits a block larger than `BLOCK_SIZE` (1 MiB,
`fs.rs:17`). Fix the comparison; add a regression test feeding a
>= 512 KiB frame at half-full and asserting no emitted block exceeds
`BLOCK_SIZE`. The new atomic writer assumes blocks <= BLOCK_SIZE.

Acceptance: existing tests green; new regression test; no `unwrap` on
any commit path in `write_path.rs`.

## Component 2: Block path layout (full-id names, adaptive depth)

```text
blocks/<hex b0>/.../<hex b(d-1)>/<full-hex id>        d in 1..=width(id)
```

Directory names are single hex bytes of the id (2 chars each, taken
from the id's leading bytes); the filename is the full-width lowercase
hex of the id (32 chars for the default 16-byte ids, 64 for 32-byte --
`BLOCKID_SIZE`/`MAX_BLOCKID_SIZE`,
`cas-storage/src/metastore/block.rs:14-17`). Dir names (2 chars) and
file names (>= 32 chars) can never collide; the `_xx` underscore
convention dies.

The load-bearing property: a file's name identifies its block, so two
hashes can never collide on a path and the depth choice has no
correctness content -- any depth is correct. Depth is performance
placement only.

- **Record change**: replace the stored path `Vec<u8>` with
  `depth: u8` (`block.rs:137` and the codec). `disk_path(id, depth,
  root)` is a pure function (`block.rs:216-227` rewritten). GET and
  DELETE use the recorded depth -- they never probe.
- **Placement policy** (performance-only): choose the shallowest depth
  whose target directory's approximate occupancy is below a threshold
  (default 4096 entries). In-memory lazy counters; approximation is
  harmless because placement carries no correctness. Depth is bounded
  only by the id width, so the fanout deepens for as long as the store
  grows.
- **Insert-time probe**: walk the id's directory chain from the blocks
  root -- at each existing level, stat `<id>` in that directory; stop
  at the first missing child directory (any candidate file must lie on
  the existing chain, so this bounds the probe to the chain's actual
  depth, worst case width(id)). If a file named `<id>` is found at
  depth d0, choose d0 (the rename then heals the orphan in place,
  complete or corrupt alike). Otherwise use the policy depth. Cost: a
  handful of stats along an existing dir chain, new-block path only.

Remove:
- the `_PATHS` tree and everything that touches it: the
  shortest-free-prefix allocator in `write_block`
  (`cas-storage/src/metastore/meta_store.rs:663-727`), the path-map
  maintenance in `delete_path.rs:19-25`, `path_tree()` accessors
  (`fs.rs`, `shared_block_store.rs`);
- any API parameter that existed only to thread path bytes around.

No migration: the record format change ships as-is. Check
`store_header.rs` for a format constant; if one exists, bump it so an
old store fails loudly at open instead of misreading records.

Acceptance: `grep -r _PATHS cas-storage/ s3cas/` returns nothing;
PUT/GET/DELETE round-trips green; a hand-planted
orphan file at depth 1 is healed in place by a retried PUT (record
stores depth 1); GET resolves via the recorded depth only.

## Component 3: Stripe set

New module `cas-storage/src/cas/stripes.rs`:

```rust
pub(crate) struct Stripes { locks: Vec<Arc<tokio::sync::Mutex<()>>> }
impl Stripes {
    fn new(n: usize) -> Self;                       // n from config, default 1024
    fn for_hash(&self, id: &BlockId) -> Arc<tokio::sync::Mutex<()>>;
}
```

Index = first two bytes of the id interpreted big-endian, modulo N.
Return the `Arc` clone so callers can use `lock_owned()` (required by
hard rule 4). N is a store-options knob, default 1024; document the
sizing rule from the ADR (spurious serialization is `(K-1)/N` per
writer; scale N to ~16x max concurrent block writers).

**Placement**: one `Stripes` on `SharedBlockStore`
(`shared_block_store.rs:12-23`) -- every namespace of one store shares
it. Two preconditions from the review, both to be handled here:
- **Blocks root moves onto `SharedBlockStore`** (today file paths derive
  from per-CasFS `fs_root`, `fs.rs:117-122`; nothing ties namespaces to
  one root, and divergent roots would break cross-namespace dedup into
  dangling reads). `SharedBlockStore` owns the blocks dir and
  `blocks/.tmp`; `CasFS` gets accessors.
- `CasFS::single_namespace` (`fs.rs:177-183`) mints a private
  `SharedBlockStore` per call. Add a doc comment: a process must not
  open the same on-disk store through it twice.

Acceptance: unit test that two namespaces over one `SharedBlockStore`
resolve the same stripe and same disk path for one id.

## Component 4: Atomic file writer

Replaces the body of the `AsyncFileSystem` seam
(`cas-storage/src/cas/async_fs.rs`). Rename the trait to something
honest (e.g. `BlockDiskWriter`); keep the injection point --
`test_store_object_write_failure` (`fs.rs:520-543`) must survive with a
mock that fails the write.

Per-block write sequence, executed inside `spawn_blocking`:
1. `create_dir_all` the fanout dir.
2. Open `blocks/.tmp/<full-hex id>-<nonce>` with `O_CREAT|O_EXCL`
   (collision = loud error, not sharing an inode). Nonce = process-wide
   `AtomicU64` counter; uniqueness per attempt is the point (a
   cancelled attempt's detached writer must never share a temp inode
   with a retry).
3. Write all bytes; fsync the file (per Durability).
4. Fsync newly created fanout directories deepest-up until an ancestor
   already known durable (per Durability). Keep an in-process
   known-durable set (`HashSet<PathBuf>` or a 256+65536 bitmap), seeded
   empty at open. `Fdatasync` still uses full fsync for directories.
5. `rename` over the final path unconditionally.
6. Fsync the parent dir (per Durability).

Store-open duties (once, before serving):
- create `blocks/` and `blocks/.tmp`; fsync both and the parent of
  `blocks/` (per Durability);
- purge `blocks/.tmp/*` (crash residue is garbage by definition;
  open-time only -- never a runtime sweeper);
- same-device check: compare `st_dev` of `blocks/` and `blocks/.tmp`,
  refuse to start on mismatch, error naming both paths and both device
  numbers. Runtime `EXDEV` from rename is logged as a store-level
  fault.

`Buffer` durability skips every fsync in this component (steps 3, 4, 6
and the open-time ones). Unlink support: `remove_file` that maps
`ENOENT` to `Ok(())`.

Acceptance: mock-writer test asserting the exact fsync set per
durability level (Buffer: none; Fsync/Fdatasync: file + new dirs +
parent); temp purge test; st_dev check test (skip if not constructible
in CI).

## Component 5: Write path reorder

`cas-storage/src/cas/write_path.rs`, the per-block closure inside
`store_object`. Target protocol:

```text
guard = stripes.for_hash(h).lock_owned().await
spawn_blocking(move ||  {          # closure owns `guard`
    tx = begin_transaction()       # sync; fjall guard inside, awaitless
    if let Some(rec) = tx.get_block(h):
        rec.rc += 1                # ALWAYS -- key_has_block is gone
        tx.commit()?               # dedup hit: no file I/O
        return Bumped
    drop(tx)
    d = probe h's dir chain for a file named h,       # component 2:
        else placement-policy depth                   # heal-in-place
    write temp; fsync; fsync dirs;
    rename to disk_path(h, d); fsync parent           # component 4
    tx = begin_transaction()
    tx.insert_block(h, rc=1, depth=d)  # no re-check needed: only
    tx.commit()?                       # stripe-holders insert, we hold it
    return Written
})                                 # guard drops inside the closure
```

Everything -- the dedup RMW, the file I/O, and the insert tx -- lives
in one `spawn_blocking` closure that owns the stripe guard. That gives
three properties at once: every fjall commit (and its journal fsync at
Fsync durability) leaves the executor; rename+insert are uncancellable
as a unit (cancellation of the request drops nothing mid-protocol --
the detached closure runs to completion and releases the guard); and
the `Send`-soundness rule holds trivially (begin and commit on one
blocking thread, no await anywhere between). Residue of a cancelled
request: a completed bump (rc over-count, leak class) or a completed
insert (record+file with no object, leak class) -- never a torn state.

Also in this component:
- delete `cleanup_on_failure` (`write_path.rs:184-195`) outright --
  with file-first ordering there is no committed record to compensate;
- remove `key_has_block` plumbing end to end: the old-object read
  (`write_path.rs:78-82`), the per-chunk computation (`:123-127`), the
  `write_block` parameter and its skip branch
  (`meta_store.rs:636-659`);
- `BlockWriteGuard` metrics state machine survives unchanged
  (`Pending -> written/failed/dropped`, `block_ignored` for dedup
  hits);
- all failures map into the closure error channel (component 1a
  style).

Acceptance: existing write-path tests adapted and green; the mock
write-failure test now asserts NO record exists after a failed write
(file-first: nothing to clean up); dedup-hit test asserts rc bump with
no disk write.

## Component 6: Delete path

`cas-storage/src/cas/delete_path.rs` plus a new atomic object-removal
seam in the namespace store.

```text
delete_object(bucket, key):
    # step 1 -- namespace DB (separate fjall database from blocks)
    obj = namespace tx { read object record; remove it; commit }
    #   atomic pair: defeats double-DELETE
    if obj is None: return Ok      # idempotent

    # step 2 -- per block occurrence, one stripe at a time
    for h in obj.blocks:
        guard = stripes.for_hash(h).lock_owned().await
        spawn_blocking(move || {   # closure owns `guard`
            tx = begin_transaction()
            match tx.get_block(h):
                None => { drop(tx) }            # already gone
                Some(rec) if rec.rc == 1 => {
                    tx.remove_block(h); tx.commit()?;
                    remove_file(disk_path(h, rec.depth))  # ENOENT == Ok
                }
                Some(rec) => { rec.rc -= 1; tx.insert(rec); tx.commit()? }
        })
        # per-block failure: log, continue with remaining blocks --
        # never abort the loop, never panic (replaces the .expect at
        # delete_path.rs:18)
```

Ordering object-removal-then-decrements means a crash between the two
steps leaves rc over-counts: leakage, never loss. The decrement, the
record removal, and the unlink share one stripe hold inside one
blocking closure (hard rule 4). `bucket_delete` keeps its inherited
shape (bucket meta removed first; mid-loop failure residue is ADR
0005's half-deleted-bucket fixture). The Content-MD5-mismatch rollback
in `s3fs.rs` (~line 714) goes through this same primitive.

Acceptance: delete tests green; double-DELETE same key on the tx
backend: second call returns Ok having done nothing; unlink of a
missing file does not error.

## Component 7: docs

ADR 0007 was accepted 2026-07-31 (remove fjall_notx) and the removal
landed before this plan's execution, so the notx warning/scope updates
this component once carried are moot. What remains:

- `docs/refcount.md`: the loss-never contract holds unconditionally
  (single backend); drop any per-backend asterisk.
- After landing, update `docs/arch/deadlock-fix.md`'s "current state"
  framing and `docs/as-built/02-storage-model.md` to describe the
  file-first protocol; record the landing commit hash in ADR 0006's
  Status line (already Accepted 2026-07-31).

## Component 8: Observability

- Gauge for in-flight block disk operations (increment when a blocking
  closure is submitted, decrement on completion; the gap to observed
  completion rate is the queue-depth signal the ADR requires). Lives in
  `cas-storage/src/metrics.rs` next to the existing block counters.
- Keep `BlockWriteGuard` counters as-is.

## Component 9: Tests and benchmark

Race stress (multi-thread runtime; assert exact rc):
- N concurrent PUTs of one new block: exactly one file write, rc == N.
- PUT storm vs DELETE-last-ref loop on one block: invariants -- never
  a record without a complete file, never an unlinked live block; at
  quiesce rc == live references.
- dedup-bump vs delete of the same block, tight loop (the defect-5
  interleaving).
- double-DELETE of one key, concurrent.

Cancellation fixtures:
- drop a PUT future at the stripe await and at the blocking await:
  residue must be nothing, a bump, or record+file -- never a temp at a
  final path, never a record without a complete file;
- cancelled DELETE cannot unlink after a subsequent PUT recreated the
  block (guard-in-closure property).

Crash-window fixture helpers for ADR 0005 (construct, assert, leave for
fsck tests): orphan file without record; orphan at a non-policy depth
(depth-change residue -- a file named `<id>` at depth d1 while the live
record says d2, or no record at all); `.tmp` residue; Buffer-mode
dangling record (record without file).

Durability: the component-4 fsync-set assertions per level.

Benchmark: extend `benches/casfs_benchmark.rs` with a concurrent-PUT
scenario (before/after numbers quantify the executor win; vary writer
count K to observe the `(K-1)/N` stripe-collision tail).

---

## Sequencing

1. Component 1 (prerequisites) -- one commit, independently green.
2. Component 2 (path layout) -- touches codec + all path call
   sites; big but mechanical; green tests before proceeding.
3. Component 3 (stripes + blocks-root move to SharedBlockStore).
4. Component 4 (atomic writer + open-time duties).
5. Component 5 (write path) and 6 (delete path) -- the concurrency
   core; land together or write-path-first with delete following
   immediately (between them the old delete still runs against the new
   write path: the meta-first assumptions it embodied are gone, so do
   not linger in that state).
6. Components 7-8 (scope docs, metrics).
7. Component 9 grows alongside 4-6; the benchmark lands last.

Each step: `make fmt` before commit, `make clippy-check && make test`
green before push. Commit messages reference ADR 0006.

## Risk ranking

1. **Components 5+6** -- the concurrency core; every verified loss bug
   lives or dies here. Mitigation: the race-stress tests are written
   against the invariants, not the implementation; run them under
   `--release` and with high iteration counts locally.
2. **Component 2** -- record-format change plus a fan-out of call-site
   edits; mechanical but wide. Mitigation: land before any behavior
   change so failures bisect cleanly.
3. **Component 4** -- filesystem/durability semantics (dir-fsync chain,
   EXCL, st_dev); platform-sensitive. Mitigation: the mock asserts the
   fsync set; manual spot-check on ext4 and btrfs if available.
4. **Components 1, 3, 7, 8** -- low risk, mostly mechanical.
