# Architecture (As-Built)

## Crate topology

```
qss_storage/  (workspace, edition 2024 for every member)
|
+-- cas-storage/       lib   27784 lines / 53 files
|     Content-addressed storage. The only crate that touches disk.
|     Both frontends and the fsck tool are built on it.
|
+-- s3cas/             bin    4916 lines /  9 files
|     S3-compatible HTTP server + inspect/check/retrieve CLI.
|     Depends on cas-storage and the s3s crate.
|
+-- respcas/           bin    6206 lines / 16 files
|     Redis/RESP2 server. Depends on cas-storage.
|
+-- qss-storage-fsck/  bin    1021 lines /  2 files
|     Offline reconciliation, scrub and repair (ADR 0005). A CLI over
|     `cas_storage::scrub`; every walker and repair lives in the library.
|
+-- benches/ (`qss-benches`)   598 lines /  2 files
      criterion benchmarks, a real workspace member with `[[bench]]`
      targets since the 2026-07-30 remediation pass.
```

Line and file counts are `find -name '*.rs'` over each crate at commit
`62fdf27` (2026-08-04), tests included; they are a rough size signal, not a
metric anything gates on.

Dependency direction is strictly one-way: the three binaries depend on
`cas-storage`, and `cas-storage` depends on none of them. There is no shared
code between `s3cas` and `respcas` other than through the library.

## Provenance: where `cas-storage` came from

`cas-storage/` was not originally written here. It was vendored from
`github.com/threefoldtech/s3-cas` at commit `b28eac0` (2026-05) and extended
for `respcas`.

**That lineage is history, not a live constraint.** The ownership decision of
2026-07-30, recorded at the top of `cas-storage/EXTENSIONS.md`, is that
qss_storage is the primary home of this code: upstream and the older lineages
it descends from are obsolete, nothing is upstreamed, there is no rebase to
protect, and the directory may be refactored freely. `EXTENSIONS.md` was
reframed to match -- it is titled "cas-storage provenance and change record"
and its upstreaming sketch is explicitly marked historical.

What survives of the fork discipline is nine `tfstor-extension: BEGIN/END`
marker regions across seven files (`metastore/traits.rs` x2,
`metastore/bucket_meta.rs`, `metastore/meta_store.rs`,
`metastore/stores/test_utils.rs` x3, `cas/multipart.rs`,
`cas/block_stream.rs`). They are a change record, not a fence: the ADR 0002
format work, the ADR 0006 write protocol and the ADR 0007 backend removal all
rewrote upstream files wholesale without adding markers, and the markers that
once lived in `stores/fjall.rs` are gone because the code they fenced was
dissolved by the backend dedup. `tfstor` is the former name of this
repository; the marker string is left alone because renaming it would be
churn for no functional gain.

The one thing a reader still needs from the fork is a reading aid: the
"What was fixed" and "What diverged wholesale" sections of `EXTENSIONS.md`
say which behaviours differ from the vendored snapshot, which is useful when
an upstream comment in the tree describes something the code no longer does.

## Process model

Two servers and one offline tool. They do not talk to each other and there is
no coordinating daemon. ADR 0004 (still Proposed) is the topology question of
which process owns which store; in practice one process holds a store
exclusively, enforced by fjall's LOCK file.

**s3cas** (`s3cas/src/main.rs`) is a clap CLI with four subcommands:

- `server` -- runs the S3 HTTP service. hyper + `hyper-util` serving an
  `s3s::service::S3Service`, with `S3Cas` (`s3cas/src/api.rs:44`) as the `S3`
  trait implementation. `run()` (`main.rs:360`) is 175 lines and also spawns
  the stale-upload GC task (ADR 0003) and the Prometheus endpoint.
- `inspect num-keys | disk-space | header` -- read-only metadata queries.
  `num-keys` reads the namespace DB (`<meta_root>/db`); `disk-space` and
  `header` report on both that and the shared block DB
  (`<meta_root>/blocks/.db`, dot-prefixed so it can never collide with the
  0xdb fanout directory).
- `check` -- integrity checking (`s3cas/src/check.rs`): every block file
  re-hashed with the store's hasher, the assembled object re-hashed with MD5
  against its ETag.
- `retrieve` -- object extraction (`s3cas/src/retrieve.rs`).

**respcas** (`respcas/src/main.rs`) is a tokio TCP server speaking RESP2.
`respcas/src/server.rs::process` is a 15-line delegate to a `Session`
(`server.rs:102`), whose `run` loop is 56 lines; `respcas/src/conn.rs` frames,
`respcas/src/resp.rs` encodes/decodes, `respcas/src/cmd.rs` parses and
dispatches. It was called `respd` until 2026-08-02 (`7d812da`); documents
dated before that keep the old name.

**qss-storage-fsck** (`qss-storage-fsck/src/main.rs`) is the offline checker
(ADR 0005). It never creates a store, writes and flushes its report before
running any repair, and inherits exclusivity from fjall's LOCK file. Exit
codes are the scripting contract: 0 clean or INFO-only, 1 WARN, 2 CRITICAL, 3
could-not-run. `--re-pair` is the ADR 0012 pairing recovery and runs no
passes.

## Request paths

### s3cas PUT object

```
hyper -> s3s S3Service -> S3Cas::put_object (s3cas/src/api.rs:1112)
  -> CasFS::store_object (cas-storage/src/cas/write_path.rs::store_object)
     -> BufferedByteStream chunks the body into 1 MiB buffers
     -> per block: BLAKE3 -> BlockId -> dedup lookup -> stage a temp file
     -> at the batch cap and at the request's end: fdatasync the staged
        files, take the stripes in sorted order, rename, dirsync, then ONE
        transaction carrying every insert and rc bump, then ONE persist
     -> object metadata written last (Object in metastore/object.rs)
```

The ordering is deliberate and matches the refcount contract in
`docs/refcount.md`: block references must be established before the object
that depends on them becomes visible, and a block record becomes readable
only after its file is durable at its final path. Leaking a block is
acceptable; losing one is not. ADR 0010 widened the unit from one block to
one request without changing that ordering; ADR 0011 optionally merges the
closing step of concurrent requests into one transaction
(`cas-storage/src/cas/group_commit.rs`).

### s3cas GET object with Range

```
S3Cas::get_object -> RangeRequest (cas-storage/src/cas/range_request.rs)
  -> CasFS read path (cas-storage/src/cas/read_path.rs)
     -> BlockStream (cas-storage/src/cas/block_stream.rs)
        Stream impl, poll_next is 149 lines, opens block files lazily
        and seeks within the first block to honour the range start.
```

### respcas SET / GET

```
tokio TCP -> conn framing -> Command::from_frame (respcas/src/cmd.rs:400)
  -> CommandHandler::execute (async since ADR 0014)
     -> Namespace (respcas/src/namespace.rs) -> Storage (respcas/src/storage.rs)
        -> CasFS / MetaStore
```

`respcas` adds a namespace layer that `s3cas` does not have: namespaces map
onto buckets, with per-namespace properties (`respcas/src/property.rs`)
covering password protection, WORM and locking, plus the key mode ADR 0014
added.

Since ADR 0014 respcas is a block-addressing store, not a metadata-only one.
A namespace whose `key_mode` is `Cas` keys every record by the BLAKE3-256 of
its value; values above the inline threshold go through the same ADR 0006
write path s3cas uses, and a respcas data directory is the same meta+blocks
pair every other store here is. Details in
[02-storage-model.md](./02-storage-model.md#content-addressed-namespaces-adr-0014)
and [03-crates.md](./03-crates.md#respcas).

## Multi-tenancy: SharedBlockStore

`cas-storage` supports two deployment shapes, both documented in the crate
docs at `cas-storage/src/lib.rs:16-71`.

**Single namespace** (`CasFS::single_namespace`, `cas/fs.rs:172`): one
metadata tree set, one block store, one tenant. This is what `s3cas server`
opens.

**Multi namespace** (`SharedBlockStore` + one `CasFS` per namespace,
`cas-storage/src/cas/shared_block_store.rs`): block storage and the `_BLOCKS`
refcount tree are shared across tenants, while each tenant gets an isolated
metadata root. This is what makes deduplication work *across* users.

`CasFS::over_namespace` (`cas/fs.rs:200`) is the third constructor, added by
ADR 0014: it builds the block engine over a `MetaStore` the caller already
opened, which is how respcas gets one without opening its metadata database
twice.

All three take paths positionally and everything else in one `StoreOptions`
(`cas-storage/src/store_options.rs`). That struct replaced the eleven- and
nine-argument constructors mid-ADR-0014, for the reason its module doc gives:
adjacent `Option<usize>` parameters are how a caller silently swaps two
settings.

Cross-tenant dedup is what gave the old MD5 addressing its teeth. Blocks are
BLAKE3-addressed since ADR 0002, default width 32 bytes; the 16-byte width is
documented as a trusted-tenant option. See
[04-code-health.md](./04-code-health.md#h4-md5-content-addressing-under-shared-block-storage----by-inspection).

## Metrics

A clean two-layer split, not duplication despite the two files named
`metrics.rs`:

- `cas-storage/src/metrics.rs` (121 lines) defines the `MetricsCollector`
  trait, a `NoOpMetrics` implementation and the `SharedMetrics` handle. The
  library depends on no metrics backend.
- `s3cas/src/metrics.rs` (510 lines) provides the Prometheus-backed collector
  and `MetricFs`, an `impl S3` wrapper that counts requests around the inner
  service.

`respcas` and `qss-storage-fsck` do not wire metrics; they get `NoOpMetrics`
via `SharedMetrics::default()`.

## Storage backend abstraction

`cas-storage/src/metastore/traits.rs` defines the seam:

- `Store` -- opens trees, begins transactions, reports disk space, counts keys
- `BaseMetaTree` -- per-tree get/insert/remove, plus `len`/`is_empty`
- `MetaTreeExt` -- iteration: `iter_all`, `iter_kv(start_after)`,
  `iter_kv_backward(start_key)`, `range_filter`
- `Transaction` / `TransactionBackend` -- write transactions

One implementation, selected by the `StorageEngine` enum
(`cas-storage/src/cas/fs.rs:45`) and exposed as the `--metadata-db` CLI flag,
defaulting to `fjall`:

| Engine | File | Lines | Transactions |
|--------|------|-------|--------------|
| `fjall` | `metastore/stores/fjall.rs` | 899 | yes, single-writer |

`fjall_notx` was removed by ADR 0007; the enum survives as the config and CLI
surface, and `FromStr` rejects the removed value with a migration message
(`fs.rs:56-60`). The interim `fjall_common.rs` flavor layer that had
deduplicated the two backends was folded back into `fjall.rs` at the same
time. Historical context:
[04-code-health.md](./04-code-health.md#b1-two-near-copy-store-backends).

## Build and CI

`.github/workflows/build.yaml` runs, on push and pull request to `main` and
`development`:

```
cargo fmt --all -- --check
cargo build --workspace
cargo clippy --workspace --all-features -- -Dwarnings
cargo test --workspace
```

No explicit toolchain install step: rustup reads `rust-toolchain.toml`, which
pins 1.97 with `rustfmt` and `clippy`, deliberately rather than tracking
stable (clippy `-D warnings` is toolchain-sensitive).

`.github/workflows/release.yaml` runs `cargo test --workspace` first, then
builds `-p s3cas -p qss-storage-fsck` in release mode for Linux and macOS and
attaches the four binaries to the tag's release. `respcas` is not currently a
release artifact. `[profile.release]` sets `lto = true` and
`codegen-units = 1`.
