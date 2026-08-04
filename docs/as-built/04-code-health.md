# Code Health: Bloat and Smell Findings

Findings from a review of branch `refactor/cas-storage` at `e349d9d`
(2026-07-30). **This is a dated record, not a current review.** The finding
text below is kept as written; what has changed since is recorded in the
status banner on each finding and summarized in the live table under
"Status at `62fdf27`".

Each finding states what was actually verified *at the time*. **Confirmed**
means reproduced or read directly off the code. **By inspection** means read
but not executed. **Needs decision** means the code is defensible and the
question is intent.

Much of what follows lives inside `cas-storage/`, which was vendored from
`threefoldtech/s3-cas @ b28eac0`. The review weighed several findings against
a rebase cost. That weighing is obsolete: the ownership decision of
2026-07-30 (top of `cas-storage/EXTENSIONS.md`) makes this the primary home
of the code, with no rebase to protect. Where a finding argues from rebase
cost, read that argument as void.

Baseline at review time: the branch was clean under
`cargo clippy --workspace --all-targets -- -D warnings` and passed 44 tests.
Everything below is beyond what the gating lints catch.

### Status legend

| Marker | Meaning |
|--------|---------|
| **RESOLVED** | the code no longer does what the finding describes; the fixing commit or ADR is named |
| **OBSOLETE** | the finding's premise no longer exists (the code, file or backend it describes is gone) |
| **CLOSED** | investigated and deliberately not changed; the reason is recorded |
| **OPEN** | still true at `62fdf27` |
| **PARTIAL** | the named instances are fixed; the class has recurred or was never fully closed |

---

## Resolution status (2026-07-30, branch `development`)

A remediation pass worked through the suggested order of work. The sections
below are kept as written (point-in-time record). **This table is itself now
dated**: it was the live status on 2026-07-30, and ten more ADRs were
implemented after it (0003, 0005-0008, 0010-0014). The live status is the
next table, "Status at `62fdf27`". Commits are on `development`.

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
| H9 async_trait in metrics | **Closed, no change** -- the finding's premise was wrong: `metrics.rs:258` is `impl S3 for MetricFs<T>`, the same upstream `#[async_trait]`-defined `s3s::S3` trait as `api.rs:83`, not a local trait. Every impl must match the macro-expanded boxed-future signatures, and s3s pulls the crate in regardless. Both occurrences stay until s3s moves to native AFIT | -- |
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
`content_md5` is still destructured and discarded in `s3cas/src/api.rs`, so
client-supplied Content-MD5 remains unverified and that finding stays open
(since fixed in the remediation pass; see the status table above).

The pass also added machinery this review predates and which is now the
place to look first when a store misbehaves: a 32-byte QSST header on every
metadata database (refusal, not migration, on a mismatch), `s3cas inspect
header` to print it, `s3cas check` verifying block files against the store's
own hasher, and an opt-in `verify_on_read` that re-hashes whole blocks
before serving them.

---

## Status at `62fdf27` (2026-08-04)

Every finding re-checked against the code as it stands. This is the live
table; the two above are dated records.

| Finding | Status | Evidence |
|---------|--------|----------|
| H1 reachable panic | **RESOLVED** | `stores/fjall.rs:324` opens the partition and counts through a read transaction. `58ca932` |
| H2 transmute + Send/Sync | **RESOLVED** | SAFETY argument at the transmute (`fjall.rs:312`), field order flagged load-bearing, `unsafe impl Sync` deleted with a `const _` in its place (`fjall.rs:645`, `:655`). `16591a5` |
| H3 pointer-width format | **RESOLVED** | `metastore/constants.rs` and `PTR_SIZE` do not exist; every on-disk length/count is `u64`. `a0c6471` |
| H4 MD5 content addressing | **RESOLVED** | blocks are BLAKE3-addressed, default width 32. `23ed542`, ADR 0002 |
| H5 `unsafe impl Sync for BlockStream` | **RESOLVED** | deleted; `const _` static assertion at `block_stream.rs:206`. `16591a5` |
| H6 `from_utf8_unchecked` | **RESOLVED** | zero occurrences in the workspace; the three surviving mentions are comments recording what upstream did. `16591a5` |
| H7 Content-MD5 unverified | **RESOLVED** | `parse_content_md5` (`api.rs:180`) applied on both `put_object` and `upload_part`. `bf2ee1a` |
| H8 `num_keys` unwrap | **RESOLVED** | `MetaStore::num_keys` returns `Result` (`meta_store.rs:416`). `58ca932` |
| H9 `#[async_trait]` | **CLOSED** | still present at `api.rs:241` and `metrics.rs:1,354`; both are `impl S3`, and `s3s` defines that trait with the macro. Unchanged and correct |
| H10 truncating casts | **PARTIAL** | the 12 audited sites are fixed (`382886e`), but the lint reports 18 again at HEAD as the workspace roughly tripled. Several are in test code; none re-open a client-influenced path the audit closed |
| H11 mixed module style | **RESOLVED** | `metastore.rs`, `stores.rs` and `scrub.rs` are all the post-2018 form. `2cab26b` |
| H12 `Durability` names transposed | **OBSOLETE** | the mapping was corrected (`7d61158`), then ADR 0010 deleted the `fdatasync` level. Two levels remain and `Fsync -> SyncAll` is right |
| B1 two near-copy backends | **OBSOLETE** | the shared half was extracted (`541cc5d`), then ADR 0007 removed `fjall_notx` entirely (`af42256`) and the flavor layer was folded back in (`4610bde`). One backend, 899 lines |
| B2 oversized functions | **PARTIAL** | the two named outliers are fixed -- `from_frame` is a 29-line dispatch table, `process` a 15-line delegate. `store_object` is 54 lines. But `poll_next` is still 149, `s3cas::main::run` grew 131 -> 175, and `clippy::too_many_lines` now fires 7 times (5 in production code) |
| B3 edition split / toolchain | **RESOLVED** | every crate is edition 2024; `rust-toolchain.toml` pins 1.97 with a comment saying why. `e4a795b`, `0777c36` |
| B4 pedantic and TODO backlog | **PARTIAL** | the substantive clusters were cleared (`4a90277`) and TODO/FIXME markers went 16 -> 9. Pedantic is 758 warnings at HEAD against 458 then, on a workspace ~3x the size; the mix is unchanged (missing backticks, `must_use`, `# Errors` sections) and still not worth chasing |
| P1 ADRs describe a layout `main` lacks | **OPEN on `main`** | `origin/main` is still `c12f930`: separate `metastore` crate, `respd`, `s3fs.rs`. Fourteen ADRs now describe code only `development` has |
| P2 `main` red in CI | **OPEN on `main`** | same cause as P1; not re-run, since the tree it describes is unchanged |
| P3 missing CI gates | **RESOLVED** | `build.yaml` runs fmt, build, clippy `-Dwarnings`, test; `release.yaml` tests before building. `0777c36`, `50ec0ec` |
| P4 `.gitignore` too narrow | **RESOLVED** | `/target`, `/data`, `.vscode`, `/qss_storage.toml`. `0777c36` |
| P5 dangling deadlock-fix doc | **OBSOLETE** | `docs/arch/deadlock-fix.md` was reconstructed (`8e554b0`, rewritten `eade068`, corrected `b95180b`), and the file that cited it -- `cas/async_fs.rs` -- was itself deleted by ADR 0006's write-path reorder (`c5e2561`) |
| P6 DBSIZE untested / naming | **RESOLVED** | `test_dbsize` in `respcas/tests/integration_test.rs:234`; `EXTENSIONS.md` records both the `tfstor` and the `respd` former names |

Verification at `62fdf27`: `cargo fmt --all --check` clean,
`cargo clippy --workspace --all-targets -- -D warnings` clean, `cargo test
--workspace` 455 passing / 0 failing / 1 ignored (the ignored one is
`batch_smoke_ab`, which writes hundreds of MiB with real fsyncs).

Two findings deserve a note beyond the table:

- **H10** is the only one whose *class* came back. The audit fixed the sites
  that existed; nothing gates the lint, so new code reintroduces them. Either
  turn `cast_possible_truncation` on in CI or stop claiming the class is
  closed. This document now claims the sites, not the class.
- **B2** measured the wrong thing to begin with. Line count found
  `from_frame`, which was genuinely bad, and also `poll_next`, which is a
  hand-rolled `Stream` state machine that does not decompose usefully.
  `s3cas::main::run` grew because ADR 0003's GC task and ADR 0012's pairing
  warning went into it; it is a bootstrap function, and long bootstraps are
  not the same problem as long dispatchers.

---

## Correctness and soundness

The sections that follow are the 2026-07-30 text verbatim, each opened by a
status banner checked against `62fdf27`. Line references inside the finding
text are the ones the reviewer read; they are NOT current.

### H1. `unimplemented!()` reachable on default flags -- CONFIRMED

> **RESOLVED** (`58ca932`). `Store::num_keys` on the fjall backend is
> `stores/fjall.rs:324`: it opens the named partition and counts through a
> read transaction, exactly as the finding proposed. `test_num_keys` in the
> shared backend battery is the regression guard; `EXTENSIONS.md` records
> the fix against upstream.

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

> **RESOLVED** (`16591a5`). The transmute is still there and still necessary
> (`fjall.rs:312`), but it now carries the two-fact SAFETY argument the
> finding asked for, the field order is flagged as load-bearing on the
> struct, and `unsafe impl Sync` is gone -- `fjall.rs:645` is a comment
> saying so, backed by a `const _` static assertion at `:655`.
> `unsafe impl Send` stays, with the honest version of its argument
> (including the part that rests on no caller holding a transaction across
> an `.await`). That transmute and that one impl are the only `unsafe` left
> in `cas-storage`.

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

> **RESOLVED** (`a0c6471`, ADR 0002). `metastore/constants.rs` does not
> exist and `PTR_SIZE` appears nowhere in the workspace. Every on-disk
> length, count and refcount field is a fixed `u64` LE; `metastore/codec.rs`
> gives all record types checked offset arithmetic and exact-length
> enforcement; golden byte vectors pin each layout. The store header
> supplies the version detection the finding said was missing.

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

> **RESOLVED** (`23ed542`, ADR 0002). Blocks are addressed by BLAKE3,
> default width 32 bytes, recorded in the store header and immutable for the
> life of the store. Untrusted multi-tenancy requires the default W32 store;
> the 16-byte width is documented as a trusted-tenant option, so opting into
> it opts out of this guarantee. No dedup-hit content verification was
> built -- the ADR's "As implemented" section says why.
>
> The finding's last paragraph -- that MD5 survives for the S3 ETag and
> carries no such risk -- is still true and is the reason `md-5` is still a
> `cas-storage` dependency. `ContentHash`
> (`metastore/content_hash.rs`) is the 16-byte newtype that keeps the ETag
> from being confused with a block address, and it is also the record
> envelope field respcas fills in for content-addressed writes where it
> carries no authority.

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

> **RESOLVED** (`16591a5`). Deleted. `block_stream.rs:193` is the comment
> recording it, and `:206` a `const _` static assertion so a future
> non-`Sync` field fails at the definition rather than being papered over.
> It was never needed: every field is already `Sync`, including `open_fut`,
> whose boxed future carries an explicit `+ Send + Sync` bound.

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

> **RESOLVED** (`16591a5`). `from_utf8_unchecked` appears nowhere in the
> workspace; the three surviving mentions are comments in
> `bucket_meta.rs:91`, `stores/fjall.rs:544` and `cas/multipart.rs:131`
> recording what upstream did and what replaced it. Record decoders go
> through `Reader::utf8` and return `FsError::InvalidUtf8`; the
> `range_filter` site logs and skips, because the trait method's item type
> is infallible. The `fjall_notx.rs` site in the finding's list went away
> with the backend.

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

> **RESOLVED** (`bf2ee1a`). `parse_content_md5` (`api.rs:180`) rejects a
> malformed header as `InvalidDigest`; a mismatch is `BadDigest`. Both
> `put_object` (`api.rs:1134`) and `upload_part` (`api.rs:1237`) apply it --
> the inlined path checks before storing, the streamed path rolls the object
> back, and `upload_part` fails before the part is registered. Missing
> Content-Length no longer panics `put_object` either.

`s3cas/src/api.rs:676`

```rust
content_md5: _, // TODO: Verify
```

S3 clients send `Content-MD5` so the server can reject corrupted uploads. It is
destructured and discarded, so that end-to-end integrity check silently does
nothing. Low effort to implement given the write path already computes the
object MD5.

### H8. `unwrap()` on a fallible store call in a facade method

> **RESOLVED** (`58ca932`). `MetaStore::num_keys` returns
> `Result<usize, MetaError>` (`meta_store.rs:416`). No in-tree caller needed
> changing -- `s3cas inspect` goes through `Store::num_keys` directly -- so
> it was a free signature change here and a breaking one for upstream's
> public API, which `EXTENSIONS.md` records.

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

> **CLOSED, no change.** Both occurrences survive at `api.rs:241` and
> `metrics.rs:1,354`, and both are correct: they are `impl S3 for S3Cas` and
> `impl S3 for MetricFs<T>`, the same upstream `s3s::S3` trait, which `s3s`
> declares with `#[async_trait::async_trait]`. Every impl must match its
> macro-expanded boxed-future signatures, and `s3s` pulls the crate in
> regardless, so hand-writing `Pin<Box<dyn Future>>` shims would remove
> nothing from the dependency tree. Revisit only if `s3s` moves to native
> AFIT. Nothing else in the workspace uses the crate.

`s3cas/src/api.rs:83`, `s3cas/src/metrics.rs:1,258`

House rule is native AFIT over the `async-trait` crate. Two cases, different
verdicts:

- `api.rs:83` -- `impl S3 for S3Cas`. The `s3s` crate defines the `S3` trait
  with `#[async_trait]`; the impl must match. Not removable without an upstream
  change to `s3s`. Leave it.
- `metrics.rs:1,258` -- a local trait impl. Likely convertible to native
  `async fn` in trait, or to `-> impl Future` if a `dyn` bound is needed.

Resolution (2026-07-30): the second verdict was wrong. `metrics.rs:258` is
`impl S3 for MetricFs<T>` -- the same upstream `s3s::S3` trait as `api.rs:83`,
not a local trait. Both impls must carry the macro to match its expanded
boxed-future signatures, and `s3s` depends on `async-trait` regardless, so
hand-writing `Pin<Box<dyn Future>>` shims would remove nothing from the
dependency tree. Closed with no code change; revisit only if `s3s` migrates
to native AFIT.

Worth noting `cas-storage/src/cas/async_fs.rs:6` records that `async_trait` was
already removed there as dead weight, so the direction of travel is established.

### H10. Truncating casts in size and offset arithmetic -- by inspection

> **PARTIAL** (`382886e` for the sites; the class is open). The twelve sites
> were audited individually: client-influenced ones (`put_object`
> Content-Length, `BlockStream` range seek/read, `upload_part` size) became
> `try_from` with an error, provably-bounded ones carry an `#[allow]` with
> the bounding argument, and `debug_assert` comparisons were flipped to the
> lossless direction. Those fixes hold.
>
> The lint is not clean at `62fdf27`: `-W clippy::cast_possible_truncation`
> reports 18 sites, in `block_stream.rs`, `gc.rs`, `uploads.rs`, `block.rs`,
> `scrub/passes.rs`, `api.rs`, `respcas/src/{cmd,namespace,resp}.rs` and
> test modules. The workspace roughly tripled in the meantime and nothing
> gates the lint, so new code reintroduces the pattern. None of the new
> sites re-open a path the audit closed, but the honest reading is that the
> SITES were fixed and the CLASS was not.

12 `clippy::cast_possible_truncation` hits under `-W clippy::pedantic`,
including `size as u64`, `part_number as i64`, and `count as i64` in
`s3cas/src/api.rs`. In a storage system these sit on the paths that compute
object sizes, part numbers, and content ranges, where a silent truncation is a
data-integrity bug rather than a display glitch. Worth auditing the 12
individually and using `try_into()` with an error where the value is
externally influenced.

### H11. Mixed module style within one crate -- cosmetic

> **RESOLVED** (`2cab26b`). Converted by `git mv`. The crate is uniformly
> post-2018: `cas.rs` + `cas/`, `metastore.rs` + `metastore/`, `stores.rs` +
> `stores/`, and `scrub.rs` + `scrub/` was written that way from the start.

`cas-storage/src/cas.rs` + `cas/` (post-2018 form) sits next to
`cas-storage/src/metastore/mod.rs` (pre-2018 form). Pick one.

### H12. `Durability` names appear transposed -- needs decision

> **OBSOLETE** (`7d61158`, then ADR 0010). The mapping was first corrected to
> match POSIX -- `Fsync -> SyncAll`, `Fdatasync -> SyncData` -- with the
> default moved to `fsync`, so the persist behaviour was bit-for-bit
> unchanged and only explicit flag users saw a difference. ADR 0010 then
> deleted the `fdatasync` level outright: once the sync boundary moved to
> the ack, the two syscalls were indistinguishable in speed and in crash
> safety, so the knob named a dead tradeoff. Two levels remain, `fsync` and
> `buffer`, and the parser refuses the removed name with the two valid ones
> in the message. There is nothing left to name wrongly.

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

> **OBSOLETE** (`541cc5d`, then ADR 0007 at `af42256`, then `4610bde`). The
> shared half was first extracted to `stores/fjall_common.rs`, generic over
> a `FjallFlavor` trait. ADR 0007 then removed the non-transactional backend
> entirely, and with one flavor left the generic layer was folded back in:
> `stores/fjall.rs` is 899 lines and is the only backend. `StorageEngine`
> survives as the config and CLI surface with one variant, and `FromStr`
> rejects `fjall_notx` with the migration path (use `fjall` with
> `durability = "buffer"`).
>
> Both caveats the finding raised are void. The rebase argument died with
> the 2026-07-30 ownership decision. The "both backends earn their
> existence" argument was answered by ADR 0007 on its merits: the notx
> backend's weaker guarantees were not a tier anyone wanted once durability
> became a per-store setting.

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

> **PARTIAL** (`721b53a`, `cfb271b`). At `62fdf27` the same five, measured
> the same way: `from_frame` 388 -> 29 (a dispatch table over per-command
> parsers, `cmd.rs:400`), `process` 212 -> 15 (a delegate to `Session`),
> `store_object` 161 -> 54 (`write_path.rs:607`), `poll_next` 147 -> 149
> (`block_stream.rs:215`), `s3cas::main::run` 131 -> 175 (`main.rs:360`).
> `clippy::too_many_lines` fires 7 times, 5 of them in production code
> (`block_stream.rs`, `group_commit.rs`, `object.rs`, `scrub/engine.rs`,
> `main.rs`).
>
> The two the finding singled out are fixed. The two that grew are a
> hand-rolled `Stream` state machine and a bootstrap function that absorbed
> ADR 0003's GC task and ADR 0012's pairing warning; neither is the
> one-`match`-does-everything shape that made `from_frame` worth splitting.
> The `// TODO: Fix this crap` the finding quoted from `poll_next` is gone.

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

> **RESOLVED** (`e4a795b`, `0777c36`). `[workspace.package]` declares edition
> 2024 and every member inherits it; `cas-storage` keeps an explicit `2024`
> that now agrees with the default. `rust-toolchain.toml` pins channel 1.97
> with `rustfmt` and `clippy`, and carries a comment saying the pin is
> deliberate because CI gates on toolchain-sensitive lints.

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

> **PARTIAL** (`4a90277`). The substantive clusters were cleared:
> `unnecessary_wraps` and `unused_self` sites fixed, `once_cell::Lazy` ->
> `std::sync::LazyLock`, dead internal macros deleted (only `try_!` was
> used, and its `TODO: remove` went with them), the bucket-count `FIXME`
> replaced by a real count at startup, `CreateBucketOutput.location` filled.
> Note one item of that pass was later reversed on purpose: respcas's
> handlers were de-async'd because fjall is sync, and ADR 0014 made
> `CommandHandler::execute` async again, because the block engine is not.
>
> The counts at `62fdf27`: 758 pedantic warnings (was 458) on a workspace
> roughly three times the size, and 9 `TODO`/`FIXME` markers (was 16). The
> pedantic mix is unchanged and still stylistic -- 126 missing backticks,
> 116 `must_use_candidate`, 109 missing `# Errors` sections, 70
> `uninlined_format_args`. Of the behaviour-tied TODOs the finding tabled,
> `api.rs:51` and `api.rs:676` are gone, `internal_macros.rs:2` is gone,
> `fs.rs`'s "very much not optimal" and `meta_store.rs`'s `list_buckets`
> pagination remain (now `fs.rs:327` and `meta_store.rs:313`), as does the
> metrics multiple-instance note (`metrics.rs:130`), left open as a
> single-instance-by-design decision.

458 warnings under `-W clippy::pedantic --all-targets`. Most are stylistic
(`doc_markdown`, `must_use_candidate`, `uninlined_format_args`) and not worth
chasing. The substantive clusters are H10 (truncating casts) and a handful of
`unused_async`, `unused_self`, `unnecessary_wraps`, and `non_std_lazy_statics`.

16 `TODO`/`FIXME` markers across the workspace. The ones tied to behaviour
rather than style:

| Location | Note |
|----------|------|
| `s3cas/src/api.rs:51` | `FIXME` -- bucket count hardcoded to 1 |
| `s3cas/src/api.rs:676` | Content-MD5 unverified (H7) |
| `s3cas/src/api.rs:200,256` | output structs returned as `default()` |
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

> **OPEN on `main`**, resolved on `development` (`9a2d8c8`). `origin/main` is
> still `c12f930`: a separate `metastore` crate, `respd/` rather than
> `respcas/`, `s3cas/src/s3fs.rs` rather than `api.rs`, and two ADRs rather
> than fourteen. Everything this document describes lives on `development`.
> The gap has widened rather than closed since the finding was written --
> merging is a decision, not a documentation task.

`docs/adr/0001-initial-architecture-overview.md` (9 references to
`cas-storage`) and `0002` (5 references) document the consolidated three-crate
layout. On `main` that layout does not exist -- `main` still has a separate
`metastore` crate. The ADRs were committed ahead of the refactor they describe.

Resolved by merging this branch. Until then, `main`'s architecture
documentation describes code that is not in `main`.

### P2. `main` is red in CI

> **OPEN on `main`**, green on `development`. Not re-verified: `main`'s tree
> is byte-identical to what the reviewer observed, so the six clippy errors
> are still in it. `development` is clean under the gate, including benches.
> Same cause and same fix as P1.

`.github/workflows/build.yaml` runs
`cargo clippy --workspace --all-features -- -Dwarnings`. On `main`'s layout,
clippy reports 6 errors in the `metastore` crate (unnecessary parentheses
around types x3, `io::Error::other`, and `unwrap` after `is_some` x2). Observed
directly when a local pre-commit hook ran clippy against `main`'s tree.

This branch fixes all six -- five by deleting the crate that contained them,
one by the auto-deref fix carried in `e349d9d`.

### P3. Missing CI gates

> **RESOLVED** (`0777c36`, `50ec0ec`). `build.yaml` runs
> `cargo fmt --all -- --check`, build, `clippy --workspace --all-features
> -- -Dwarnings` and `test --workspace`, on push and pull request to both
> `main` and `development`. There is no explicit toolchain install step,
> deliberately: rustup reads `rust-toolchain.toml`. `release.yaml` runs
> `cargo test --workspace` as a gating job before either build job, and lost
> its deprecated `actions-rs` stable-override.

- No `cargo fmt --check`. Formatting drift is currently caught only by local
  hooks, inconsistently.
- No `rust-toolchain.toml` (see B3).
- `release.yaml` builds but does not test.

### P4. `.gitignore` is too narrow

> **RESOLVED** (`0777c36`). Now `/target`, `/data`, `.vscode` and
> `/qss_storage.toml` -- the last so an operator's local config cannot be
> committed over the example.

Contents are `/target` and `.vscode`. Notably absent: the `data/` directory
that the servers write into by default. In the pre-rename checkout, `data/`
existed as untracked-but-committable content -- one `git add -A` from being
committed. Adding `/data` is a one-line fix.

### P5. Dangling documentation reference

> **OBSOLETE.** Fixed twice over. The rationale was reconstructed from the
> code as `docs/arch/deadlock-fix.md` (`8e554b0`, rewritten from primary
> sources at `eade068`, corrected against the ADR 0006 review at `b95180b`);
> the original document and the commit that was supposed to carry it exist
> in no reachable history. Then the file that cited it was itself deleted:
> ADR 0006's write-path reorder (`c5e2561`) removed `cas/async_fs.rs` and
> the `AsyncFileSystem` trait, replacing the injection seam with
> `BlockDiskOps` in `cas/block_disk.rs`. The reconstructed document is kept
> because it explains a deadlock that was real.

`cas-storage/src/cas/async_fs.rs:6` cites `docs/arch/deadlock-fix.md`. Neither
that file nor `docs/arch/` exists; `docs/` holds only `refcount.md` and
`adr/`. Either the document was never carried over from upstream or it was
lost. Since it explains why the `AsyncFileSystem` abstraction exists at all,
the missing rationale is worth reconstructing.

### P6. Test and naming gaps

> **RESOLVED** (`cfb271b`, `9dabfe1`). `test_dbsize` is at
> `respcas/tests/integration_test.rs:234`. `EXTENSIONS.md` carries the
> naming note the finding proposed, and has since gained a second one:
> `respd` is the former name of the `respcas` crate and binary (renamed
> 2026-08-02, `7d812da`), so documents dated before that keep the old name.
> The `tfstor-extension` markers are still spelled that way, still on
> purpose.

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

> **Historical.** Items 1 and 3 through 8 were all worked; item 2 (merging
> to `main`) was not, and is the only entry still live. What is left of this
> review at `62fdf27` is P1/P2 on `main`, the recurrence of H10's class, and
> the two long functions under B2 that were never the point.

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
