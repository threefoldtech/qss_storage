# Code Health: Bloat and Smell Findings

Findings from a review of branch `refactor/cas-storage` at `e349d9d`.

Each finding states what was actually verified. **Confirmed** means reproduced
or read directly off the code. **By inspection** means read but not executed.
**Needs decision** means the code is defensible and the question is intent.

Much of what follows lives inside `cas-storage/`, which is a vendored fork of
`threefoldtech/s3-cas @ b28eac0`. Those findings are upstream's, not this
repository's authorship -- but they are this repository's risk, and fixing them
locally has a rebase cost. Where that tension applies it is called out.

Baseline: this branch is clean under
`cargo clippy --workspace --all-targets -- -D warnings` and passes 44 tests.
Everything below is beyond what the gating lints catch.

---

## Resolution status (2026-07-30, branch `development`)

A remediation pass worked through the suggested order of work. The sections
below are kept as written (point-in-time record); this table is the live
status. Commits are on `development`.

| Finding | Status | Commit(s) |
|---------|--------|-----------|
| H1 reachable panic | **Fixed** -- `FjallStore::num_keys` delegates to `read_tx().len()`; regression test in the shared backend battery | `58ca932` |
| H2 transmute + Send/Sync | **Addressed** -- SAFETY argument written, field order marked load-bearing, `unsafe impl Sync` deleted (auto impl suffices), `Send` kept with an honest argument | `16591a5` |
| H3 pointer-width format | **Fixed** -- format v1: all length/count fields are `u64`, `PTR_SIZE` and `constants.rs` deleted, records length-exact, block-id lists self-describing (width byte); golden byte vectors pin the layout. No backward compatibility: pre-v1 stores read as decode errors until the store header lands | `a0c6471` |
| H4 MD5 cross-tenant substitution | **Fixed** -- recorded in ADR 0002 first (`9dabfe1`), then closed: blocks are BLAKE3-addressed, default width 32. Untrusted multi-tenancy now requires the default W32 store; the 16-byte width is documented as a trusted-tenant option, so a deployment that opts into it opts out of this guarantee. No dedup-hit content verification was built -- see the ADR's "As implemented" section for why | `23ed542` (with `43d6594`, `03c5cf5`) |
| H5 BlockStream Sync | **Fixed** -- deleted; static assertion in its place | `16591a5` |
| H6 unchecked UTF-8 | **Fixed** -- all six sites validate; `range_filter` sites log-and-skip (trait signature unchanged, see EXTENSIONS.md) | `16591a5` |
| (H2 adjacent) FjallNoTransaction unsafe impls | **Fixed** -- both redundant, deleted with static assertions | `a5722c8` |
| H7 Content-MD5 unverified | **Fixed** -- malformed headers rejected as `InvalidDigest`; mismatches return `BadDigest` (inlined path checks before storing, streamed path rolls the object back, upload_part fails before the part is registered). Bonus: missing Content-Length no longer panics `put_object` | -- |
| H8 num_keys unwrap | **Fixed** -- returns `Result`; no in-tree caller needed changes | `58ca932` |
| H9 async_trait in metrics | **Closed, no change** -- the finding's premise was wrong: `metrics.rs:258` is `impl S3 for MetricFs<T>`, the same upstream `#[async_trait]`-defined `s3s::S3` trait as `s3fs.rs:83`, not a local trait. Every impl must match the macro-expanded boxed-future signatures, and s3s pulls the crate in regardless. Both occurrences stay until s3s moves to native AFIT | -- |
| H10 truncating casts | **Fixed** -- audited individually: client-influenced sites (`put_object` Content-Length, `BlockStream` range seek/read, `upload_part` size) now `try_from` with an error; provably-bounded sites carry `#[allow]` with the bounding argument; `debug_assert` comparisons flipped to the lossless direction. `-W clippy::cast_possible_truncation` is clean | -- |
| H11 module style | **Fixed** -- `metastore/mod.rs` and `metastore/stores/mod.rs` converted to the post-2018 `metastore.rs` / `stores.rs` form via `git mv` | -- |
| H12 Durability naming | **Fixed, then moot** -- the mapping was swapped to match POSIX semantics (`Fsync` -> `SyncAll`, `Fdatasync` -> `SyncData`) and the default moved to `fsync`; ADR 0010 then deleted the `fdatasync` level outright, so there is nothing left to name wrongly. Two levels remain: `fsync` and `buffer` | `7d61158` |
| B1 duplicated backends | **Fixed** -- shared half extracted to `stores/fjall_common.rs`, generic over a `FjallFlavor` trait; the two stores are aliases of it with unchanged public API. The rebase argument is moot per the ownership decision at the top of `cas-storage/EXTENSIONS.md` | `541cc5d` |
| B2 oversized functions | **Fixed** -- `from_frame` 388 -> 47-line dispatch table with per-command parsers; `process` -> 10-line delegate to `Session` | `721b53a`, `cfb271b` |
| B3 edition split / toolchain | **Fixed** -- workspace on edition 2024, toolchain pinned 1.97 | `e4a795b`, `0777c36` |
| B4 pedantic backlog | **Substantives cleared** -- respcas handlers/execute/dispatch de-async'd (fjall is sync; the async was decorative), `unnecessary_wraps` and `unused_self` sites fixed, `once_cell::Lazy` -> `std::sync::LazyLock`, dead internal macros deleted (only `try_!` was used), bucket-count FIXME fixed (real count at startup), `CreateBucketOutput.location` filled. Left open as design decisions: metrics double-registration (single-instance by design), `list_buckets` pagination, `bucket_delete` optimization. Stylistic pedantic noise stays unchased | -- |
| P1 ADRs describe unmerged layout | **Resolved on `development`** -- merged at `9a2d8c8`; `main` stays stale until development merges back | `9a2d8c8` |
| P2 main red in CI | Same as P1 -- green on `development` | `9a2d8c8` |
| P3 missing CI gates | **Fixed** -- fmt gate added, toolchain pin honored, CI runs on development, stray checkout dropped; `release.yaml` now runs `cargo test --workspace` before building and lost its deprecated `actions-rs` stable-override (the pin in rust-toolchain.toml applies) | `0777c36`, `50ec0ec` |
| P4 .gitignore | **Fixed** -- `/data` ignored | `0777c36` |
| P5 dangling deadlock-fix doc | **Fixed** -- rationale reconstructed from the code as `docs/arch/deadlock-fix.md` (the original document and commit `c5f9cc9` exist in no reachable history); `async_fs.rs` comment rewritten to drop the dangling references | -- |
| P6 DBSIZE untested / naming | **Fixed** -- `test_dbsize` added; former-name note in EXTENSIONS.md | `cfb271b`, `9dabfe1` |

Found and fixed during the pass, beyond the original findings:

- The criterion benchmarks were dead code: not a workspace member, no
  `[[bench]]` target anywhere, still importing pre-refactor paths. Now a
  `qss-benches` workspace member crate; both binaries compile, run, and are
  covered by the clippy gate (`cd535e3`).
- respcas's integration test harness had a real port-allocation race
  (bind-drop-rebind), the cause of a rare one-in-N test failure. The listener
  is now bound once and moved into the server thread; sleep-based readiness
  waits removed. Suite wall time 19s -> ~1s (`cfb271b`).

Verification on `development` after the pass: fmt clean, clippy
`-D warnings` clean including benches, 47 tests passing (twice), both bench
binaries run to completion.

### ADR 0002 implementation pass (2026-07-30)

The BLAKE3 migration specified in
[docs/adr/0002-blake3-hash-migration.md](../adr/0002-blake3-hash-migration.md)
landed in fourteen steps (C1-C14) over commits `8ed2506`..`69c726e`, per
[docs/plans/adr-0002-implementation.md](../plans/adr-0002-implementation.md).
It closes two findings from this review: **H3** (on-disk format v1, all
length and count fields `u64`, at `a0c6471`) and **H4** (blocks addressed by
BLAKE3, default 32 bytes, at `23ed542`). It does not touch **H7**:
`content_md5` is still destructured and discarded in `s3cas/src/s3fs.rs`, so
client-supplied Content-MD5 remains unverified and that finding stays open
(since fixed in the remediation pass; see the status table above).

The pass also added machinery this review predates and which is now the
place to look first when a store misbehaves: a 32-byte QSST header on every
metadata database (refusal, not migration, on a mismatch), `s3cas inspect
header` to print it, `s3cas check` verifying block files against the store's
own hasher, and an opt-in `verify_on_read` that re-hashes whole blocks
before serving them.

---

## Correctness and soundness

### H1. `unimplemented!()` reachable on default flags -- CONFIRMED

`cas-storage/src/metastore/stores/fjall.rs:132-135`

```rust
fn num_keys(&self, _: &str) -> Result<usize, MetaError> {
    unimplemented!("fjall with transaction does not support number of keys");
}
```

Reached from `s3cas inspect num-keys` via `s3cas/src/inspect.rs:23`. The
`--metadata-db` flag defaults to `fjall` (`s3cas/src/main.rs:55`), so the
default invocation panics. Reproduced:

```
$ s3cas inspect --meta-root /tmp/meta num-keys testbucket
thread 'main' panicked at cas-storage/src/metastore/stores/fjall.rs:133:9:
not implemented: fjall with transaction does not support number of keys

$ s3cas inspect --meta-root /tmp/meta --metadata-db fjall_notx num-keys testbucket
Number of keys in bucket 'testbucket': 0
```

**The panic message is factually wrong**, which makes this cheaper to fix than
it looks. Two hundred lines later in the same file, `BaseMetaTree::len` does
exactly the counting the message claims is impossible:

```rust
fn len(&self) -> Result<usize, MetaError> {
    let read_tx = self.db.read_tx();
    let len = read_tx.len(&*self.partition)...
}
```

So the fix is to open the named partition and delegate to `read_tx().len()`,
mirroring `FjallStoreNotx::num_keys`. No fjall limitation is involved.

Scope is limited to the CLI. `respcas`'s `DBSIZE` takes a different route
(`respcas/src/namespace.rs::num_keys` -> `BaseMetaTree::len`) and does **not**
hit this, so there is no network-reachable panic here. Verified by reading both
paths.

### H2. Lifetime laundering plus unaudited `Send`/`Sync` -- by inspection

`cas-storage/src/metastore/stores/fjall.rs:119-131, 156-157`

```rust
let tx = unsafe {
    std::mem::transmute::<fjall::SingleWriterWriteTx<'_>, fjall::SingleWriterWriteTx<'static>>(
        self.db.write_tx(),
    )
};
...
unsafe impl Send for FjallTransaction {}
unsafe impl Sync for FjallTransaction {}
```

The `'static` claim holds in current code, for reasons the comment does not
give: `FjallTransaction` stores an `Arc<FjallStore>`, whose `Arc<...Database>`
clone keeps the database alive for at least as long as the transaction, and
field declaration order (`tx` before `store`) makes the transaction drop first.
Both are load-bearing and neither is written down, so a future field reorder or
a change to what `FjallTransaction` owns would break soundness silently.

The `Send`/`Sync` impls are the larger concern. `SingleWriterWriteTx` is
presumably not `Send`/`Sync` by design -- the type name states a single-writer
invariant. Asserting both removes the compiler's enforcement of exactly the
property fjall is trying to guarantee. `Send` is plausibly needed to hold the
transaction across an `await` in a tokio task. `Sync` is harder to justify:
every `TransactionBackend` method takes `&mut self`, so shared-reference access
should not arise.

Suggested: document the drop-order and Arc-liveness argument as a `SAFETY`
comment, add a compile-time guard against field reordering, and try deleting
`unsafe impl Sync` to see whether anything actually needs it.

### H3. On-disk format depends on host pointer width -- by inspection

`cas-storage/src/metastore/constants.rs:4`, used in 19 places across
`block.rs`, `bucket_meta.rs`, `multipart.rs`.

```rust
pub const PTR_SIZE: usize = mem::size_of::<usize>();
```

Length and refcount fields are serialized as native-width `usize`, so a store
written on a 64-bit host is not readable on a 32-bit one, and there is no
version or magic byte to detect the mismatch. Full analysis in
[02-storage-model.md](./02-storage-model.md#pointer-width-dependence-resolved).

This matters more than usual here because the product goal is aggregating
storage across a heterogeneous node network -- 32-bit ARM or RISC-V nodes make
it concrete rather than theoretical, as does any attempt to replicate metadata
between nodes.

Fix is `u64` for all on-disk length and count fields. That is format-breaking,
so it belongs with the ADR 0002 migration, which already plans a format
transition.

### H4. MD5 content addressing under shared block storage -- by inspection

`cas-storage/src/cas/write_path.rs` (hashing),
`cas-storage/src/cas/shared_block_store.rs` (sharing)

Blocks are content-addressed by MD5, and in `SharedBlockStore` mode the block
store and `_BLOCKS` refcount tree are shared across namespaces (tenants).
Deduplication is therefore cross-tenant: if tenant A writes a block whose MD5
matches one tenant B already stored, A's write is deduplicated onto B's block.

MD5 chosen-prefix collisions are practical. So a tenant who can upload
arbitrary bytes can construct two distinct blocks with the same MD5 and, in the
shared mode, cause one tenant's data to be served in place of another's. The
refcount design means whichever block landed first wins and the second writer's
content is silently discarded.

This is a property of MD5 plus cross-tenant dedup, not a coding defect, and
ADR 0002 already proposes BLAKE3 -- but ADR 0002 is **Status: Proposed** and
frames the motivation as MD5 being "chosen for simplicity and speed". The
multi-tenant substitution consequence is worth recording explicitly in that
ADR, because it changes the migration from a nice-to-have into a prerequisite
for offering shared-block multi-tenancy to untrusted tenants.

Interim mitigations, if BLAKE3 is not imminent: do not enable
`SharedBlockStore` across trust boundaries, or verify full block content on
dedup hit rather than trusting the digest.

Note this is separate from MD5's use for S3 ETags, which is mandated by S3
compatibility and carries no such risk.

### H5. `unsafe impl Sync` with no justification -- by inspection

`cas-storage/src/cas/block_stream.rs:46`

```rust
unsafe impl Sync for BlockStream {}
```

No `SAFETY` comment, no explanation. `BlockStream` holds an `open_fut` future
and a file handle and is polled as a `Stream`. Whether `Sync` is sound depends
on what `open_fut` contains; whether it is *needed* is a separate question that
the code does not answer. Same recommendation as H2: try removing it and see
what breaks.

### H6. `from_utf8_unchecked` on data read back from disk -- by inspection

Six sites: `bucket_meta.rs:97`, `stores/fjall.rs:390`,
`stores/fjall_notx.rs:315`, `multipart.rs:93,108,124`.

```rust
// SAFETY: this is safe because we only store valid strings in the first place.
name: unsafe { String::from_utf8_unchecked(value[8 + PTR_SIZE..].to_vec()) },
```

The safety argument covers the write path but not the read path. These bytes
come back from a database file, where the invariant can be broken by disk
corruption, a truncated write, a partially-migrated format, or a version that
wrote a different layout -- exactly the H3 scenario. The consequence of a
violated invariant is undefined behaviour rather than an error return.

The performance argument is weak: `str::from_utf8` is a fast SIMD-friendly
validation, and these are bucket names, keys, and upload IDs, not bulk data.
Recommend `String::from_utf8` mapped into the existing `MetaError`/`FsError`,
which every one of these call sites is already positioned to return.

---

## Design and consistency

### H7. Client-supplied Content-MD5 accepted and ignored -- by inspection

`s3cas/src/s3fs.rs:676`

```rust
content_md5: _, // TODO: Verify
```

S3 clients send `Content-MD5` so the server can reject corrupted uploads. It is
destructured and discarded, so that end-to-end integrity check silently does
nothing. Low effort to implement given the write path already computes the
object MD5.

### H8. `unwrap()` on a fallible store call in a facade method

`cas-storage/src/metastore/meta_store.rs:363`

```rust
pub fn num_keys(&self) -> usize {
    self.store.num_keys(DEFAULT_BUCKET_TREE).unwrap()
}
```

Swallows the `Result` into a panic in a method whose doc comment says it is
"primarily used for monitoring and debugging". Compounds H1. Should return
`Result<usize, MetaError>`.

### H9. `#[async_trait]` still present -- needs decision

`s3cas/src/s3fs.rs:83`, `s3cas/src/metrics.rs:1,258`

House rule is native AFIT over the `async-trait` crate. Two cases, different
verdicts:

- `s3fs.rs:83` -- `impl S3 for S3FS`. The `s3s` crate defines the `S3` trait
  with `#[async_trait]`; the impl must match. Not removable without an upstream
  change to `s3s`. Leave it.
- `metrics.rs:1,258` -- a local trait impl. Likely convertible to native
  `async fn` in trait, or to `-> impl Future` if a `dyn` bound is needed.

Resolution (2026-07-30): the second verdict was wrong. `metrics.rs:258` is
`impl S3 for MetricFs<T>` -- the same upstream `s3s::S3` trait as `s3fs.rs:83`,
not a local trait. Both impls must carry the macro to match its expanded
boxed-future signatures, and `s3s` depends on `async-trait` regardless, so
hand-writing `Pin<Box<dyn Future>>` shims would remove nothing from the
dependency tree. Closed with no code change; revisit only if `s3s` migrates
to native AFIT.

Worth noting `cas-storage/src/cas/async_fs.rs:6` records that `async_trait` was
already removed there as dead weight, so the direction of travel is established.

### H10. Truncating casts in size and offset arithmetic -- by inspection

12 `clippy::cast_possible_truncation` hits under `-W clippy::pedantic`,
including `size as u64`, `part_number as i64`, and `count as i64` in
`s3cas/src/s3fs.rs`. In a storage system these sit on the paths that compute
object sizes, part numbers, and content ranges, where a silent truncation is a
data-integrity bug rather than a display glitch. Worth auditing the 12
individually and using `try_into()` with an error where the value is
externally influenced.

### H11. Mixed module style within one crate -- cosmetic

`cas-storage/src/cas.rs` + `cas/` (post-2018 form) sits next to
`cas-storage/src/metastore/mod.rs` (pre-2018 form). Pick one.

### H12. `Durability` names appear transposed -- needs decision

`cas-storage/src/metastore/stores/fjall.rs:45-49`

```rust
Durability::Fsync      => fjall::PersistMode::SyncData,
Durability::Fdatasync  => fjall::PersistMode::SyncAll,
```

By POSIX convention `fdatasync` is the weaker call (may skip metadata) and
`fsync` the stronger. Here `Fdatasync` selects the stronger fjall mode
(`SyncAll`) and `Fsync` the weaker (`SyncData`). The default is `Fdatasync`
(`fjall.rs:44`), which is also the `s3cas` CLI default -- so the default is the
strongest mode, which is a safe default but not what the name suggests.

Either the mapping is transposed or the enum names mean something other than
the POSIX calls. Worth confirming before anyone tunes durability for
performance based on the flag names.

---

## Bloat and duplication

### B1. Two near-copy store backends

`cas-storage/src/metastore/stores/fjall.rs` (433 lines) and
`fjall_notx.rs` (358 lines): 791 lines total, of which **235 lines are
identical**, and the two files define almost the same function set (differing
only by `commit_persist`, present in the transactional one).

This is the largest single block of duplication in the workspace. Every
extension has to be written twice -- `EXTENSIONS.md` explicitly says the
`iter_kv` additions were "port both `fjall.rs` and `fjall_notx.rs` impls (copy
from this fork)", i.e. the duplication is a known, accepted tax.

Two caveats before deduplicating:

1. The duplication is upstream's. Refactoring it locally maximizes rebase pain
   against `s3-cas`, which cuts directly against the stated goal of dissolving
   the fork. This is arguably a fix to make *upstream*, not here.
2. Both backends earn their existence: `benches/fjall_benchmark.rs` exists to
   compare them, and `shared_block_store.rs:49` treats `fjall_notx` as a real
   deployment option with weaker guarantees.

So: real bloat, but the right move is probably an upstream PR rather than a
local refactor. Worth a decision either way rather than drift.

### B2. Oversized functions

Measured by brace matching, production code only:

| Lines | Location |
|-------|----------|
| 388 | `respcas/src/cmd.rs::from_frame` |
| 212 | `respcas/src/server.rs::process` |
| 161 | `cas-storage/src/cas/write_path.rs::store_object` |
| 147 | `cas-storage/src/cas/block_stream.rs::poll_next` |
| 131 | `s3cas/src/main.rs::run` |

`from_frame` is the clear outlier: one `match` handling arity checks, type
coercion, and construction for all twenty respcas commands. It is the natural
place for a per-command parse trait or a table-driven arity/type spec, and it
is not vendored code -- `respcas` is this repository's own, so there is no rebase
argument against fixing it.

`poll_next` at 147 lines is a hand-rolled state machine and carries the
workspace's bluntest comment (`block_stream.rs:117`, `// TODO: Fix this crap`).

`clippy::too_many_lines` fires 4 times under pedantic.

### B3. Edition split across the workspace

| Crate | Edition |
|-------|---------|
| workspace `[workspace.package]` | 2018 |
| `cas-storage` | 2024 (explicit override) |
| `s3cas` | 2018 (inherited) |
| `respcas` | 2018 (inherited) |

The refactor moved `cas-storage` to 2024 but left the workspace default and
both frontends on 2018 -- an edition that predates `async`/`await` stabilizing
in its current form and is three editions behind. This compiles (editions are
per-crate) but means the two binaries are written against 2018 idioms while the
library they consume uses 2024. Moving the workspace to 2024 is mostly
mechanical (`cargo fix --edition`) and worth doing while the layout is already
in flux.

There is also no `rust-toolchain.toml`, so the toolchain is whatever the builder
has -- and CI gates on `clippy -D warnings`, which is toolchain-sensitive by
nature. A new stable release can turn CI red without a code change. Pinning is
cheap insurance.

### B4. Pedantic lint and TODO backlog

458 warnings under `-W clippy::pedantic --all-targets`. Most are stylistic
(`doc_markdown`, `must_use_candidate`, `uninlined_format_args`) and not worth
chasing. The substantive clusters are H10 (truncating casts) and a handful of
`unused_async`, `unused_self`, `unnecessary_wraps`, and `non_std_lazy_statics`.

16 `TODO`/`FIXME` markers across the workspace. The ones tied to behaviour
rather than style:

| Location | Note |
|----------|------|
| `s3cas/src/s3fs.rs:51` | `FIXME` -- bucket count hardcoded to 1 |
| `s3cas/src/s3fs.rs:676` | Content-MD5 unverified (H7) |
| `s3cas/src/s3fs.rs:200,256` | output structs returned as `default()` |
| `s3cas/src/metrics.rs:109` | may crash with multiple instances |
| `cas-storage/src/metastore/meta_store.rs:189` | `list_buckets` should be paginated/streamed |
| `cas-storage/src/cas/fs.rs:199` | "this is very much not optimal" |
| `s3cas/src/internal_macros.rs:2` | `TODO: remove` |

`meta_store.rs:189` is the one with scaling teeth: an unpaginated
`list_buckets` that materializes every bucket is fine at small scale and a
problem at the scale the README describes.

---

## Process and documentation

### P1. ADRs describe a layout `main` does not have

`docs/adr/0001-initial-architecture-overview.md` (9 references to
`cas-storage`) and `0002` (5 references) document the consolidated three-crate
layout. On `main` that layout does not exist -- `main` still has a separate
`metastore` crate. The ADRs were committed ahead of the refactor they describe.

Resolved by merging this branch. Until then, `main`'s architecture
documentation describes code that is not in `main`.

### P2. `main` is red in CI

`.github/workflows/build.yaml` runs
`cargo clippy --workspace --all-features -- -Dwarnings`. On `main`'s layout,
clippy reports 6 errors in the `metastore` crate (unnecessary parentheses
around types x3, `io::Error::other`, and `unwrap` after `is_some` x2). Observed
directly when a local pre-commit hook ran clippy against `main`'s tree.

This branch fixes all six -- five by deleting the crate that contained them,
one by the auto-deref fix carried in `e349d9d`.

### P3. Missing CI gates

- No `cargo fmt --check`. Formatting drift is currently caught only by local
  hooks, inconsistently.
- No `rust-toolchain.toml` (see B3).
- `release.yaml` builds but does not test.

### P4. `.gitignore` is too narrow

Contents are `/target` and `.vscode`. Notably absent: the `data/` directory
that the servers write into by default. In the pre-rename checkout, `data/`
existed as untracked-but-committable content -- one `git add -A` from being
committed. Adding `/data` is a one-line fix.

### P5. Dangling documentation reference

`cas-storage/src/cas/async_fs.rs:6` cites `docs/arch/deadlock-fix.md`. Neither
that file nor `docs/arch/` exists; `docs/` holds only `refcount.md` and
`adr/`. Either the document was never carried over from upstream or it was
lost. Since it explains why the `AsyncFileSystem` abstraction exists at all,
the missing rationale is worth reconstructing.

### P6. Test and naming gaps

- `DBSIZE` has no test, despite being one of the two commands that motivated
  promoting `BaseMetaTree::len` out of `#[cfg(test)]`. Reading the code says it
  works (H1 explains why it takes the safe path), but nothing guards it.
- `EXTENSIONS.md` and the `tfstor-extension` markers still use the pre-rename
  name. Renaming the markers would create churn against upstream for no
  functional gain, so the sensible resolution is a one-line note in
  `EXTENSIONS.md` recording that `tfstor` is the former name of this
  repository, rather than a sweep.

---

## Suggested order of work

1. **H1** -- confirmed panic on a default code path, and the fix is a few lines
   delegating to `read_tx().len()`. Fix in place.
2. **P2 / P1** -- merge this branch. Turns CI green and makes the ADRs true.
3. **H4** -- amend ADR 0002 to record the multi-tenant substitution risk. This
   is a documentation change that may reprioritize the whole BLAKE3 migration.
4. **H6, H2, H5** -- the `unsafe` cluster. Convert `from_utf8_unchecked` to
   checked conversions, write the missing `SAFETY` arguments, and test whether
   the two `unsafe impl Sync`s are needed at all.
5. **H3** -- fold fixed-width on-disk fields into the ADR 0002 format
   transition rather than doing it standalone.
6. **B3, P3, P4** -- edition bump, toolchain pin, fmt gate, gitignore. Cheap
   and mechanical.
7. **B2 (`from_frame`)** -- own code, no rebase cost, clear win.
8. **B1** -- decide: upstream PR, or accept the duplication and stop
   relitigating it.
