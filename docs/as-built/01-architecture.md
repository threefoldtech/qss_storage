# Architecture (As-Built)

## Crate topology

```
qss_storage/  (workspace, edition 2018 at the workspace level)
|
+-- cas-storage/   lib, edition 2024   4884 lines / 27 files
|     Content-addressed storage. The only crate that touches disk.
|     Vendored fork of threefoldtech/s3-cas @ b28eac0.
|
+-- s3cas/         bin, edition 2018   2368 lines /  9 files
|     S3-compatible HTTP server + inspect/check/retrieve CLI.
|     Depends on cas-storage and the s3s crate.
|
+-- respd/         bin, edition 2018   4058 lines / 12 files
|     Redis/RESP2 server. Depends on cas-storage.
|
+-- benches/                            591 lines /  2 files
      criterion benchmarks, workspace-level (not a crate member).
```

Dependency direction is strictly one-way: both frontends depend on
`cas-storage`, and `cas-storage` depends on neither. There is no shared code
between `s3cas` and `respd` other than through the library. cmm's boundary
analysis agrees, classifying `src` as `core` with fan-in 71 and fan-out 1.

## The vendored-fork boundary

This is the single most important structural fact about the repository and it
is easy to miss.

`cas-storage/` was not written here. Per `cas-storage/EXTENSIONS.md`, it was
copied wholesale from `github.com/threefoldtech/s3-cas` at commit `b28eac0`
(2026-05). The intent is that the directory stays byte-identical to that
upstream snapshot **except** for four explicitly fenced additions, so a future
upstream rebase is a directory swap plus re-application of the fences.

The four extension blocks, all added to serve `respd`:

| Location | Addition | Consumer |
|----------|----------|----------|
| `metastore/traits.rs:48-59` | `BaseMetaTree::len` / `is_empty` promoted out of `#[cfg(test)]` | respd `LENGTH`, `DBSIZE` |
| `metastore/traits.rs:76-88` | `MetaTreeExt::iter_kv(start_after)` | respd `SCAN` cursor |
| `metastore/stores/fjall.rs:275-335` | impl of the above, transactional backend | -- |
| `metastore/stores/fjall_notx.rs:204-260` | impl of the above, non-transactional backend | -- |

Plus two `promoted out of #[cfg(test)] for runtime use` markers at
`fjall.rs:260` and `fjall_notx.rs:189`.

`EXTENSIONS.md` states the goal as upstreaming these so the fork can dissolve,
and sketches the PR. That has not happened.

Two consequences:

- Any change inside `cas-storage/` that is not fenced silently increases rebase
  cost. The four clippy fixes carried in commit `e349d9d` include one such
  unfenced edit (`metastore/stores/fjall.rs`, auto-deref).
- `EXTENSIONS.md` still calls this "the tfstor fork" and refers to the
  `tfstor` workspace. The repository was renamed to `qss_storage`; the marker
  string `tfstor-extension` is now historical. Renaming the markers would be
  churn against upstream, so the naming is probably best left alone and simply
  understood.

## Process model

Two independent binaries. They do not talk to each other and there is no
coordinating daemon.

**s3cas** (`s3cas/src/main.rs`) is a clap CLI with four subcommands:

- `server` -- runs the S3 HTTP service. hyper + `hyper-util` serving an
  `s3s::service::S3Service`, with `S3FS` (`s3cas/src/s3fs.rs`) as the `S3`
  trait implementation. `run()` is 131 lines.
- `inspect num-keys | disk-space | header` -- read-only metadata queries.
  `num-keys` reads the namespace DB (`<meta_root>/db`); `disk-space` and
  `header` report on both that and the shared block DB
  (`<meta_root>/blocks/db`).
- `check` -- integrity checking (`s3cas/src/check.rs`): every block file
  re-hashed with the store's hasher, the assembled object re-hashed with MD5
  against its ETag.
- `retrieve` -- object extraction (`s3cas/src/retrieve.rs`).

**respd** (`respd/src/main.rs`) is a tokio TCP server speaking RESP2.
`respd/src/server.rs::process` (212 lines) owns the per-connection loop;
`respd/src/conn.rs` frames, `respd/src/resp.rs` encodes/decodes,
`respd/src/cmd.rs` parses and dispatches.

## Request paths

### s3cas PUT object

```
hyper -> s3s S3Service -> S3FS::put_object (s3cas/src/s3fs.rs)
  -> CasFS::store_object (cas-storage/src/cas/write_path.rs::store_object, 161 lines)
     -> BufferedByteStream chunks the body into 1 MiB buffers
     -> per block: MD5 -> BlockID -> refcount insert-or-increment -> write to disk
     -> object metadata written last (Object in metastore/object.rs)
```

The ordering is deliberate and matches the refcount contract in
`docs/refcount.md`: block references must be established before the object
that depends on them becomes visible. Leaking a block is acceptable; losing
one is not.

### s3cas GET object with Range

```
S3FS::get_object -> parse_range_request (cas-storage/src/cas/range_request.rs)
  -> CasFS read path (cas-storage/src/cas/read_path.rs)
     -> BlockStream (cas-storage/src/cas/block_stream.rs)
        Stream impl, poll_next is 147 lines, opens block files lazily
        and seeks within the first block to honour the range start.
```

### respd SET / GET

```
tokio TCP -> conn framing -> Command::from_frame (respd/src/cmd.rs, 388 lines)
  -> Namespace (respd/src/namespace.rs) -> Storage (respd/src/storage.rs)
     -> CasFS / MetaStore
```

`respd` adds a namespace layer that `s3cas` does not have: namespaces map onto
buckets, with per-namespace properties (`respd/src/property.rs`) covering
password protection, WORM, and locking.

## Multi-tenancy: SharedBlockStore

`cas-storage` supports two deployment shapes, both documented in the crate
docs at `cas-storage/src/lib.rs:15-63`.

**Single namespace** (`CasFS::single_namespace`): one metadata tree set, one
block store, one tenant.

**Multi namespace** (`SharedBlockStore` + one `CasFS` per namespace,
`cas-storage/src/cas/shared_block_store.rs`): block storage and the `_BLOCKS`
refcount tree are shared across tenants, while each tenant gets an isolated
metadata root. This is what makes deduplication work *across* users, and it is
the mode that gives the MD5 collision finding its teeth -- see
[04-code-health.md](./04-code-health.md#h4-md5-content-addressing-under-shared-block-storage----by-inspection).

`shared_block_store.rs:49-56` emits a runtime warning when `fjall_notx` is
selected in multi-user mode, noting weaker consistency guarantees, then
proceeds.

## Metrics

A clean two-layer split, not duplication despite the two files named
`metrics.rs`:

- `cas-storage/src/metrics.rs` (73 lines) defines the `MetricsCollector` trait
  and a `NoOpMetrics` implementation. The library depends on no metrics
  backend.
- `s3cas/src/metrics.rs` (390 lines) provides the Prometheus-backed
  `SharedMetrics` and a `CasMetricsAdapter` that implements
  `cas_storage::MetricsCollector` over it.

`respd` does not wire metrics; it gets `NoOpMetrics` by default.

## Storage backend abstraction

`cas-storage/src/metastore/traits.rs` defines the seam:

- `Store` -- opens trees, begins transactions, reports disk space
- `BaseMetaTree` -- per-tree get/insert/remove, plus the promoted `len`/`is_empty`
- `MetaTreeExt` -- iteration, including the two added `iter_kv` variants
- `Transaction` / `TransactionBackend` -- write transactions

Two implementations, selected by the `StorageEngine` enum
(`cas-storage/src/cas/fs.rs:43`) and exposed as the `--metadata-db` CLI flag
on `s3cas`, defaulting to `fjall`:

| Engine | File | Lines | Transactions |
|--------|------|-------|--------------|
| `fjall` | `metastore/stores/fjall.rs` | 433 | yes, single-writer |
| `fjall_notx` | `metastore/stores/fjall_notx.rs` | 358 | no |

The two files share 235 identical lines. See
[04-code-health.md](./04-code-health.md#b1-two-near-copy-store-backends).

## Build and CI

`.github/workflows/build.yaml` runs, on push:

```
cargo build --workspace
cargo clippy --workspace --all-features -- -Dwarnings
cargo test --workspace
```

`.github/workflows/release.yaml` builds `-p s3cas` and `-p respd` in release
mode. `[profile.release]` sets `lto = true` and `codegen-units = 1`.

There is no `cargo fmt --check` gate in CI, and no `rust-toolchain.toml`
pinning a toolchain.
