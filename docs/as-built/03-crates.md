# Crate-by-Crate Detail (As-Built)

Sizes are `find -name '*.rs' | wc -l` and a line count over the same set, at
commit `62fdf27` (2026-08-04). Test modules inside `src/` are included, and in
`cas-storage` they are a large fraction of the total -- `scrub/`,
`race_tests.rs`, `crash_fixtures.rs` and the golden-vector modules together
are several thousand lines.

## cas-storage

Library crate, 27784 lines across 53 files, edition 2024. Originally vendored
from `threefoldtech/s3-cas @ b28eac0`, now owned outright -- see
[01-architecture.md](./01-architecture.md#provenance-where-cas-storage-came-from).

### Module layout

```
cas-storage/src/
+-- lib.rs                    crate docs + the public re-export surface
+-- config.rs                 qss_storage.toml loader + every default   1123
+-- hasher.rs                 the BLAKE3 Hasher enum (W16 / W32)         284
+-- metrics.rs                MetricsCollector, NoOpMetrics, SharedMetrics 121
+-- store_options.rs          StoreOptions: the resolved store settings   375
+-- cas.rs        + cas/      the CAS layer
|   +-- fs.rs                 CasFS, StorageEngine, BLOCK_SIZE          1812
|   +-- write_path.rs         the batched PUT (ADR 0006/0010)           2133
|   +-- block_disk.rs         atomic block writer + BlockDiskOps seam   1107
|   +-- uploads.rs            multipart upload lifecycle (ADR 0003)      938
|   +-- multipart.rs          MultiPart, MultiPartTree                   716
|   +-- group_commit.rs       the commit station (ADR 0011)              699
|   +-- gc.rs                 stale-upload sweep (ADR 0003)              654
|   +-- shared_block_store.rs multi-tenant shared block store            647
|   +-- clone_path.rs         clone-by-reference (ADR 0014)              450
|   +-- block_stream.rs       Stream impl over block files               372
|   +-- stripes.rs            the striped lock set (ADR 0006)            285
|   +-- placement.rs          adaptive fanout depth                      230
|   +-- delete_path.rs        deletion + refcount decrement              182
|   +-- buffered_byte_stream.rs  chunks input into BLOCK_SIZE buffers    158
|   +-- byte_stream.rs        AsyncByteStream                             39
|   +-- read_path.rs          read assembly                               38
|   +-- buckets.rs            bucket operations                           29
|   +-- range_request.rs      RangeRequest                                24
|   +-- race_tests.rs, crash_fixtures.rs, ack_visibility_tests.rs
|                             cfg(test) only
+-- metastore.rs + metastore/
|   +-- meta_store.rs         MetaStore facade, tree names              1339
|   +-- store_header.rs       the QSST header (ADR 0002/0012/0014)      1103
|   +-- object.rs             Object, ObjectData, ObjectType             921
|   +-- block.rs              Block, BlockId                             616
|   +-- upload_record.rs      UploadRecord (ADR 0003)                    328
|   +-- traits.rs             Store, BaseMetaTree, MetaTreeExt, ...      283
|   +-- bucket_meta.rs        BucketMeta                                 232
|   +-- errors.rs             MetaError, FsError                         221
|   +-- codec.rs              record cursor + id-list helpers            181
|   +-- content_hash.rs       ContentHash, the ETag newtype               46
|   +-- stores.rs + stores/
|       +-- fjall.rs          the only backend                           899
|       +-- test_utils.rs     backend test battery                       285
+-- scrub.rs + scrub/         fsck's engine (ADR 0005)
    +-- repair.rs             repair actions                            1448
    +-- passes.rs             the walkers                               1083
    +-- engine.rs             pass orchestration                         462
    +-- findings.rs           the finding taxonomy                       446
    +-- pairing.rs            store-id pairing + --re-pair (ADR 0012)    383
    +-- report.rs             report rendering (text and JSON)           375
    +-- holders.rs            reference-holder walk                      320
    +-- disk.rs               block-file walk, foreign-file rules        298
    +-- records.rs            record walk                                 78
```

The module style is consistent post-2018 throughout: `cas.rs` beside `cas/`,
`metastore.rs` beside `metastore/`, `scrub.rs` beside `scrub/`. (Finding H11
was the pre-2018 `mod.rs` files, converted by `git mv`.)

`cas/async_fs.rs` and its `AsyncFileSystem` trait no longer exist: the ADR
0006 write-path reorder replaced them with `BlockDiskOps`
(`cas/block_disk.rs:95`), which is the same injection seam for tests.

### Public surface

`lib.rs:85-155` re-exports deliberately, so consumers never name the module
path -- and since 2026-08-04 most of them cannot: `hasher`, `metrics` and
`store_options` are private modules, leaving four public
(`cas`, `config`, `metastore`, `scrub`). Every re-export is a named list --
the two globs that used to stand at
the `metastore` front (`meta_store::*` and `traits::*`) were replaced with
named lists on 2026-08-04, so a new `pub` item joins the public surface only
when somebody decides it should. Five groups: the hasher (`Hasher`,
`HasherError`), the config types
(`QssStorageConfig` and its per-section structs, `ConfigError`),
`StoreOptions`, metastore types (`MetaStore`, `Store`, `BaseMetaTree`,
`MetaTreeExt`, `Block`, `BlockId`, `BlockTree`, `BucketMeta`, `ContentHash`,
`Object`, `ObjectData`, `ObjectType`, `Durability`, `MetaError`,
`FjallStore`, `Transaction`, `StoreHeader`, `HeaderSpec`, `StoreHeaderError`,
`StoreId`), and CAS types (`CasFS`, `SharedBlockStore`, `StorageEngine`,
`AsyncByteStream`, `BlockStream`, `BlockCorruption`, `MultiPart`,
`MultiPartTree`, `RangeRequest`, `GroupCommit`, `GroupCommitStats`,
`SweepStats`, `sweep_stale_uploads`, `UploadClaim`, `BLOCKS_DB_DIR_NAME`,
`STORE_ID_MARKER_NAME`), plus metrics.

Below `lib.rs` only two modules stay public -- `cas::fs` and
`metastore::store_header`, each for items the re-exports do not carry -- and
each says so in a comment where it is declared. The storage pipeline and the
nine scrub walkers are private modules behind their re-exported names; see
[01-architecture.md](./01-architecture.md#module-fronts) for the full tree and
the house rules it follows.

Both doctests in `lib.rs` compile (`no_run`), covering the single-namespace
and multi-namespace construction paths. They are the only executable
documentation in the workspace, and they are what would break first if the
constructors changed shape again -- which is how they earned their keep when
`StoreOptions` replaced the positional arguments.

### Testing

`metastore/stores/test_utils.rs` is a backend-agnostic battery driven by a
`backend_test_battery!` macro; with one backend left it runs once, but the
seam survives. `cas/race_tests.rs`, `cas/crash_fixtures.rs` and
`cas/ack_visibility_tests.rs` are `#[cfg(test)]` modules holding the
concurrency and durability tests -- the ones that pin the ADR 0006 stripe
protocol, the ADR 0008 release-on-displace ordering and the ADR 0013 ack
classes. `scrub/tests.rs` and `scrub/repair/tests.rs` cover fsck.

One test is `#[ignore]`d: `cas::write_path::tests::batch_smoke_ab`, which
writes a few hundred MiB with real fsyncs and is run explicitly.

## s3cas

Binary crate, 4916 lines across 9 files, edition 2024.

| File | Lines | Role |
|------|-------|------|
| `tests/it_s3.rs` | 1463 | integration tests via `s3s-aws` + `aws-sdk-s3` |
| `api.rs` | 1320 | `impl S3 for S3Cas` -- the whole S3 verb surface |
| `main.rs` | 672 | clap CLI, four subcommands, server bootstrap, GC task |
| `metrics.rs` | 510 | Prometheus collector + `MetricFs` wrapper |
| `check.rs` | 382 | integrity checking |
| `inspect.rs` | 382 | `num-keys`, `disk-space`, `header` |
| `retrieve.rs` | 157 | object extraction |
| `internal_macros.rs` | 16 | the `try_!` macro |
| `lib.rs` | 12 | module wiring + the single `cas` alias for `cas_storage` |

The S3 trait implementation used to be called `S3FS` and to live in
`s3fs.rs`; it is `S3Cas` in `api.rs` since `a438277` (2026-08-03), on the
grounds that it was never a filesystem.

### Implemented S3 operations

From the `impl S3 for S3Cas` block (`api.rs:242`): `abort_multipart_upload`,
`complete_multipart_upload`, `create_bucket`, `create_multipart_upload`,
`delete_bucket`, `delete_object`, `delete_objects`, `get_bucket_location`,
`get_object`, `head_bucket`, `head_object`, `list_buckets`,
`list_multipart_uploads`, `list_objects`, `list_objects_v2`, `list_parts`,
`put_object`, `upload_part`.

`copy_object` returns `NotImplemented` (`api.rs:431-438`), with a comment
pointing at the upstream s3-cas implementation and noting uncertainty about
whether it is correct. The README's "Known issues" records this, and ADR 0008
binds its future implementation (a self-copy must short-circuit rather than
release the blocks it is about to reference).

Integration tests were parameterized over both storage engines
(`do_test_*(engine: StorageEngine)`) when there were two; the shape survives
with one.

### Notable in-code caveats

- `api.rs:518` -- `TODO: check for the key existence?` in `delete_object`.
- `api.rs:521` -- `DeleteObjectOutput` returned as `default()` with a
  "handle other fields" TODO.
- `metrics.rs:130` -- TODO noting the metrics registry may crash with
  multiple instances. Left open as a design decision: single-instance by
  design.
- `api.rs:241` and `metrics.rs:1,354` use `#[async_trait]`. Both are
  `impl S3 for ...` -- the same upstream `s3s::S3` trait, which `s3s`
  declares with `#[async_trait::async_trait]`, so every impl must match its
  expanded boxed-future signatures. Neither is removable locally, and `s3s`
  depends on the crate regardless. See finding H9. Revisit only if `s3s`
  moves to native AFIT.

The bucket-count `FIXME` and the Content-MD5 `TODO: Verify` that this section
used to list are both gone: `S3Cas::new` counts buckets for real
(`api.rs:58-61`), and `parse_content_md5` (`api.rs:180`) is applied on both
`put_object` and `upload_part`.

## respcas

Binary crate, 6206 lines across 16 files, edition 2024. A Redis/RESP2 subset
server backed by `cas-storage`. Called `respd` until 2026-08-02.

| File | Lines | Role |
|------|-------|------|
| `tests/integration_test.rs` | 1448 | the bulk of the test suite |
| `cmd.rs` | 1078 | command parsing (a dispatch table) and execution |
| `tests/cas_test.rs` | 568 | ADR 0014: mode A/B, presence, clone, worm |
| `namespace.rs` | 538 | namespace abstraction over buckets |
| `storage.rs` | 528 | `Storage`, `NamespaceMeta`, `KeyMode`, store layout |
| `resp.rs` | 467 | RESP2 frame encode/decode, inline commands, the value cap |
| `server.rs` | 393 | accept loop and the per-connection `Session` |
| `main.rs` | 216 | clap flags, config merge, bootstrap |
| `content.rs` | 194 | ADR 0014 ingest: presence, verify, clone |
| `tests/common/mod.rs` | 189 | the test harness |
| `tests/layout_test.rs` | 185 | store layout: new shape, pre-0014 shape |
| `tests/command_test.rs` | 145 | targeted command tests |
| `tests/password_test.rs` | 134 | password protection |
| `property.rs` | 58 | namespace property parsing |
| `conn.rs` | 54 | connection framing |
| `lib.rs` | 14 | module wiring; the test-access front, not an API |

### Command surface

Twenty-two commands. Standard Redis subset:

`AUTH` `DBSIZE` `DEL` `ECHO` `EXISTS` `GET` `MGET` `PING` `SCAN` `SELECT`
`SET` `TIME`

Non-standard extensions for namespace and data management:

`CHECK` `CSET` `FLUSH` `KEYTIME` `LENGTH` `NSINFO` `NSLIST` `NSNEW` `NSSET`
`RSCAN`

`RSCAN` (backward iteration) is the reason for the `iter_kv_backward`
extension to `MetaTreeExt`; `SCAN` drives `iter_kv(start_after)`; `LENGTH`
and `DBSIZE` drive the promotion of `BaseMetaTree::len` out of `#[cfg(test)]`.
`CSET` is ADR 0014's explicit spelling of the server-hashed put, equivalent
to `SET "" <value>`.

Keys are `Bytes`, not `String`, throughout the command layer: a Cas namespace
keys its records by a raw BLAKE3 digest, and a lossy UTF-8 decode on the way
in would address a different record than the client named. `ECHO` keeps its
payload as bytes for the same reason -- `valkey-cli --pipe` ends its stream
with an ECHO of twenty random bytes and waits for them back verbatim.

### Namespace semantics

Namespaces map onto `cas-storage` buckets, with properties held in
`property.rs` and persisted as msgpack `NamespaceMeta` in the `_BUCKETS`
value. The test suite exercises three protection modes -- password
(`password_test.rs`), WORM (`test_worm_protection`) and locking
(`test_lock_protection`) -- which are respcas-level policies, not enforced by
`cas-storage`.

Since ADR 0014 a namespace also has a `key_mode`: `UserKey` (the default and
what every namespace was), `Cas` (the key is the BLAKE3-256 of the value), and
`Sequential`, which is zdb heritage that nothing implements -- `NSSET
key_mode sequential` is refused as unimplemented rather than accepted and
ignored, and the variant survives only because it is on disk in stores
created with it. The mode can only be changed while a namespace is empty.

WORM composes with Cas deliberately: a content-addressed write whose address
is already present is an acknowledgement, not a modification, so
`refuse_unless_writable` skips the occupancy check for it
(`namespace.rs:254`). That is also why an ordinary write no longer pays a
read to discover that its namespace is not WORM.

### Structural concerns

`Command::from_frame` (`cmd.rs:400`) is a 30-line dispatch table over
per-command parser functions; `server.rs::process` is a 15-line delegate to
`Session`. Both were finding B2 at 388 and 212 lines respectively.

`lib.rs` is a second compilation of the same sources: `main.rs` declares its
own `mod` tree, so the binary never links against the library crate and the
library exists only so `tests/` can drive the internals. It is scoped to that
-- five modules and eighteen items, down from eight and eighty-six -- and the
list is in
[01-architecture.md](./01-architecture.md#module-fronts).

## benches (`qss-benches`)

Two criterion benchmarks, 598 lines, a real workspace member with `[[bench]]`
targets since the 2026-07-30 remediation pass (they were dead code before
that: not a member, no target, still importing pre-refactor paths).

- `fjall_benchmark.rs` (274 lines) -- benchmarks `FjallStore` across
  `insert_bucket`, small/medium object insert, `get_meta`, `list_buckets`,
  `transaction`, and a mixed workload. (It was a two-backend comparison until
  ADR 0007 removed `FjallStoreNotx`.)
- `casfs_benchmark.rs` (324 lines) -- `CasFS`-level benchmarks, including the
  `store_by_hash_width` group that runs the same object sizes at both block
  address widths.

Both are the heaviest users of `.unwrap()` outside tests, which is
appropriate for benchmark code.

## qss-storage-fsck

Binary crate, 1021 lines across 2 files. ADR 0005's offline reconciliation,
scrub and repair tool.

| File | Lines | Role |
|------|-------|------|
| `tests/fsck.rs` | 702 | end-to-end runs over damaged stores |
| `src/main.rs` | 319 | clap CLI, store open, exit-code contract |

Thin on purpose: every walker, pass and repair action lives in
`cas_storage::scrub`, so the daemon's own store is checked by the same code an
operator runs and a future online mode can wrap it. Three properties are
load-bearing in the binary rather than the library -- it never creates a store
(a mistyped `--meta-root` would otherwise produce an empty store and a clean
report about nothing), the report is written and flushed before the first
repair action, and exclusivity is inherited from fjall's LOCK file rather than
built.

`--re-pair` (ADR 0012) rewrites the blocks root's `.store-id` marker to match
the database's header, prints both ids, and runs no passes. Both roots must be
spelled out; a store whose header has no id yet is refused by the verb.
