# Crate-by-Crate Detail (As-Built)

## cas-storage

Library crate, 4884 lines across 27 files, edition 2024. Vendored fork of
`threefoldtech/s3-cas @ b28eac0` -- see
[01-architecture.md](./01-architecture.md#the-vendored-fork-boundary).

### Module layout

```
cas-storage/src/
+-- lib.rs                    crate docs + the public re-export surface
+-- metrics.rs                MetricsCollector trait, NoOpMetrics
+-- cas.rs        + cas/      the CAS layer
|   +-- fs.rs                 CasFS, StorageEngine, BLOCK_SIZE      813 lines
|   +-- write_path.rs         store_object, BlockWriteGuard          275 lines
|   +-- read_path.rs          read assembly
|   +-- delete_path.rs        deletion + refcount decrement
|   +-- shared_block_store.rs multi-tenant shared block store
|   +-- buckets.rs            bucket operations
|   +-- block_stream.rs       Stream impl over block files
|   +-- byte_stream.rs        AsyncByteStream
|   +-- buffered_byte_stream.rs  chunks input into BLOCK_SIZE buffers
|   +-- multipart.rs          MultiPart, MultiPartTree
|   +-- range_request.rs      RangeRequest, parse_range_request
|   +-- async_fs.rs           AsyncFileSystem abstraction
+-- metastore/mod.rs + metastore/
    +-- traits.rs             Store, BaseMetaTree, MetaTreeExt, Transaction
    +-- meta_store.rs         MetaStore facade, tree names             660 lines
    +-- object.rs             Object, ObjectData, ObjectType           553 lines
    +-- block.rs              Block, BlockID
    +-- bucket_meta.rs        BucketMeta
    +-- errors.rs             MetaError, FsError
    +-- codec.rs              record cursor + id-list helpers
    +-- stores/
        +-- fjall.rs          the (transactional) backend             ~600 lines
        +-- test_utils.rs     backend test battery
```

Note the mixed module style: `cas.rs` alongside `cas/` (the post-2018 form) but
`metastore/mod.rs` (the pre-2018 form). Cosmetic, but inconsistent within one
crate -- finding H11.

### Public surface

`lib.rs:70-107` re-exports deliberately, so consumers never name the module
path. Three groups: metastore types (`MetaStore`, `Store`, `BaseMetaTree`,
`Block`, `BlockID`, `BucketMeta`, `Object`, `ObjectData`, `ObjectType`,
`Durability`, `MetaError`, `FjallStore`, `Transaction`,
`MetaTreeExt`), CAS types (`CasFS`, `SharedBlockStore`, `StorageEngine`,
`AsyncByteStream`, `BlockStream`, `MultiPart`, `MultiPartTree`,
`RangeRequest`, `parse_range_request`), and metrics
(`MetricsCollector`, `NoOpMetrics`, `SharedMetrics`).

Both doctests in `lib.rs` compile (`no_run`), covering the single-namespace and
multi-namespace construction paths. They are the only executable documentation
in the workspace.

### Testing

`metastore/stores/test_utils.rs` (with `test_range_filter` at 116 lines) is a
backend-agnostic battery run against both stores, which is why the two backends
stay behaviourally aligned despite being separate implementations.
`cas/fs.rs` carries a `#[cfg(test)]` module from line 328 with a `MockFs`
implementing `AsyncFileSystem` -- correctly test-gated, not shipped.

## s3cas

Binary crate, 2368 lines across 9 files, edition 2018 (inherited from the
workspace).

| File | Lines | Role |
|------|-------|------|
| `s3fs.rs` | 751 | `impl S3 for S3FS` -- the whole S3 verb surface |
| `metrics.rs` | 390 | Prometheus `SharedMetrics` + `CasMetricsAdapter` |
| `main.rs` | 278 | clap CLI, four subcommands, server bootstrap |
| `check.rs` | -- | integrity checking |
| `inspect.rs` | -- | `num-keys`, `disk-space`, `header` |
| `retrieve.rs` | -- | object extraction |
| `internal_macros.rs` | -- | `try_!` macro; carries a `TODO: remove` |
| `lib.rs` | -- | module wiring |
| `tests/it_s3.rs` | 590 | integration tests via `s3s-aws` + `aws-sdk-s3` |

### Implemented S3 operations

From the `impl S3 for S3FS` block: `create_bucket`, `delete_bucket`,
`head_bucket`, `list_buckets`, `get_bucket_location`, `put_object`,
`get_object`, `head_object`, `delete_object`, `delete_objects`,
`list_objects`, `list_objects_v2`, `create_multipart_upload`,
`upload_part`, `complete_multipart_upload`.

`copy_object` returns `NotImplemented` (`s3fs.rs:173-180`), with a comment
pointing at the upstream s3-cas implementation and noting uncertainty about
whether it is correct. The README's "Known issues" already records this.

Integration tests are parameterized over both storage engines
(`do_test_*(engine: StorageEngine)`), which is good coverage discipline.

### Notable in-code caveats

- `s3fs.rs:51` -- `FIXME` on `metrics.set_bucket_count(1)`, a hardcoded bucket
  count standing in for a real count.
- `s3fs.rs:200`, `s3fs.rs:256` -- `CreateBucketOutput` / `DeleteObjectOutput`
  returned as `default()` with "handle other fields" TODOs.
- `s3fs.rs:676` -- `content_md5: _, // TODO: Verify`. The client-supplied
  Content-MD5 header is accepted and ignored rather than validated.
- `metrics.rs:109` -- TODO noting the metrics registry may crash with multiple
  instances.
- `s3fs.rs:83` and `metrics.rs:1,258` use `#[async_trait]`. The `s3fs.rs` one is
  forced by the `s3s` crate's trait definition and cannot be removed locally.
  The `metrics.rs` one is local and likely removable -- finding H9.

## respcas

Binary crate, 4058 lines across 12 files, edition 2018. A Redis/RESP2 subset
server backed by `cas-storage`.

| File | Lines | Role |
|------|-------|------|
| `tests/integration_test.rs` | 1481 | the bulk of the test suite |
| `cmd.rs` | 1049 | command parsing and execution |
| `namespace.rs` | 405 | namespace abstraction over buckets |
| `server.rs` | -- | connection accept loop, `process` (212 lines) |
| `storage.rs` | -- | `Storage` wrapper over `CasFS` / `MetaStore` |
| `resp.rs` | -- | RESP2 frame encode/decode, plus inline-command parsing |
| `conn.rs` | -- | connection framing |
| `property.rs` | -- | namespace properties (password, WORM, lock) |
| `main.rs`, `lib.rs` | -- | bootstrap |
| `tests/command_test.rs`, `tests/password_test.rs` | 277 | targeted tests |

### Command surface

Twenty-one commands. Standard Redis subset:

`AUTH` `DBSIZE` `DEL` `ECHO` `EXISTS` `GET` `MGET` `PING` `SCAN` `SELECT` `SET`
`TIME`

Non-standard extensions for namespace and data management:

`CHECK` `FLUSH` `KEYTIME` `LENGTH` `NSINFO` `NSLIST` `NSNEW` `NSSET` `RSCAN`

`RSCAN` (backward iteration) is the reason for the `iter_kv_backward` extension
to `MetaTreeExt`; `SCAN` drives `iter_kv(start_after)`; `LENGTH` and `DBSIZE`
drive the promotion of `BaseMetaTree::len` out of `#[cfg(test)]`.

### Namespace semantics

Namespaces map onto `cas-storage` buckets, with properties held in
`property.rs`. The test suite exercises three protection modes -- password
(`password_test.rs`), WORM (`test_worm_protection`), and locking
(`test_lock_protection`) -- which are respcas-level policies, not enforced by
`cas-storage`.

### Structural concerns

`Command::from_frame` is 388 lines (`cmd.rs`), the largest function in the
workspace, combining argument arity checking, type coercion, and construction
for all twenty commands in one `match`. `server.rs::process` is 212 lines.
Both are finding B2.

## benches

Two criterion benchmarks at the workspace root, 591 lines. Not workspace
members; they sit in `benches/` and are referenced by the workspace.

- `fjall_benchmark.rs` -- benchmarks `FjallStore` across `insert_bucket`,
  small/medium object insert, `get_meta`, `list_buckets`, `transaction`,
  and a mixed workload. (It was a two-backend comparison until ADR 0007
  removed `FjallStoreNotx`.)
- `casfs_benchmark.rs` (179 lines) -- `CasFS`-level benchmarks.

Both are the heaviest users of `.unwrap()` outside tests (40 and 9 sites), which
is appropriate for benchmark code.
