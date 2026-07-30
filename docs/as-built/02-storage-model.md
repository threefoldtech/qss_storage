# Storage Model (As-Built)

## Content addressing

Objects are split into fixed 1 MiB blocks. From `cas-storage/src/cas/fs.rs:17`:

```rust
pub const BLOCK_SIZE: usize = 1 << 20; // Supposedly 1 MiB
```

("Supposedly" is upstream's comment, not a note added here.)

Each block is hashed with **MD5**. The 16-byte digest is the `BlockID`
(`cas-storage/src/metastore/block.rs`), which serves as both the key in the
block metadata tree and the basis for the on-disk path.

ADR 0002 proposes migrating to BLAKE3 and is marked **Status: Proposed**. It is
not implemented: there is no `blake3` dependency anywhere in the workspace, and
`md-5` remains in use at `cas-storage/src/cas/write_path.rs`,
`s3cas/src/s3fs.rs`, `s3cas/src/check.rs`, and `respd/src/namespace.rs`.
The crate docs at `cas-storage/src/lib.rs:8` correctly state MD5.

MD5 is also used for a second, distinct purpose in `s3cas`: computing S3 ETags,
including the multipart "MD5 of the MD5s" construction
(`s3cas/src/s3fs.rs::calculate_multipart_hash`). That use is dictated by S3
compatibility and is not a free choice. The *content addressing* use is a free
choice, and is the one ADR 0002 targets.

## Keyspaces

Three fjall trees, named at `cas-storage/src/metastore/meta_store.rs:22-24`:

| Tree | Constant | Holds |
|------|----------|-------|
| `_BUCKETS` | `DEFAULT_BUCKET_TREE` | `BucketMeta` per bucket |
| `_BLOCKS` | `DEFAULT_BLOCK_TREE` | `Block` records, keyed by `BlockID`, carrying the refcount |
| `_PATHS` | `DEFAULT_PATH_TREE` | block path allocation |

Object metadata lives in a per-bucket tree, opened by bucket name. Multipart
upload state lives in its own tree (`cas-storage/src/cas/multipart.rs`,
`MultiPartTree`).

In `SharedBlockStore` mode, `_BLOCKS` and the block files are shared across all
namespaces while each namespace keeps its own bucket and object trees.

## On-disk record formats

Serialization is hand-rolled little-endian byte packing, not a serde format.
Each type has a `to_vec` and a `TryFrom<&[u8]>`.

### Block (`cas-storage/src/metastore/block.rs`)

```
[ size: usize LE          ]  PTR_SIZE bytes
[ path_len: u8            ]  1 byte
[ path: bytes             ]  path_len bytes
[ rc: usize LE            ]  PTR_SIZE bytes
```

Total `PTR_SIZE * 2 + 1 + path_len`, checked exactly at `block.rs:68`.

A `TODO` at `block.rs:25` notes the path could be a fixed `[u8; BLOCKID_SIZE]`
plus a length byte, avoiding the variable-length tail.

### BucketMeta (`cas-storage/src/metastore/bucket_meta.rs`)

```
[ ctime: i64 LE           ]  8 bytes
[ name_len: usize LE      ]  PTR_SIZE bytes
[ name: UTF-8 bytes       ]  name_len bytes
```

### MultiPart (`cas-storage/src/cas/multipart.rs`)

Five `PTR_SIZE` length fields plus 8 bytes plus a `BLOCKID_SIZE` hash, then the
variable tails; the minimum size check is at `multipart.rs:82`.

## Pointer-width dependence

`PTR_SIZE` is defined at `cas-storage/src/metastore/constants.rs:4` as:

```rust
pub const PTR_SIZE: usize = mem::size_of::<usize>();
```

and it appears directly in the three formats above -- 19 references across
`block.rs`, `bucket_meta.rs`, and `multipart.rs`.

**The on-disk format therefore varies with the pointer width of the host that
wrote it.** On x86_64 or aarch64, `PTR_SIZE` is 8. On a 32-bit target
(armv7, riscv32, i686) it is 4.

Concrete consequences:

- A store written on 64-bit and opened on 32-bit will misparse. The exact-length
  assertions (`block.rs:68`, `bucket_meta.rs:91`) turn most cases into a
  `TryFrom` error rather than silent corruption, which is the saving grace --
  but `bucket_meta.rs` computes `name_len` from an 8-byte field read as 4 bytes
  before that check, so the failure mode is length-dependent, not uniform.
- Metadata cannot be replicated or migrated between hosts of different pointer
  width, which matters for a system whose stated purpose is aggregating storage
  across a heterogeneous node network.
- Nothing in the format records which width wrote it, so there is no version
  or magic byte to detect the mismatch and refuse cleanly.

A fixed-width type (`u64`) for all on-disk length and refcount fields would
remove the coupling. That is a format-breaking change and so belongs with the
ADR 0002 migration, which already contemplates a format transition. Tracked as
finding H3 in [04-code-health.md](./04-code-health.md).

## Reference counting

The contract is stated in `docs/refcount.md` and is asymmetric by design:

> There can be some data leakage, but never data loss.

Concretely: failing to *increase* a refcount is forbidden, because the block
could then be collected while still referenced. Failing to *decrease* one is
acceptable, because the result is an orphaned block that wastes space but
loses nothing.

The document enumerates four failure cases and classifies each. Only the
forbidden one carries a mandatory test.

Refcount transitions, per `docs/refcount.md`:

- new object storing a new block -> rc = 1
- same key rewriting the same block -> rc unchanged
- different key referencing an existing block -> rc + 1
- object deleted -> rc - 1
- rc reaches 0 -> block deleted
- key updated to different blocks -> rc - 1 on blocks no longer referenced

The write path lives in `cas-storage/src/cas/write_path.rs`, the delete path in
`cas-storage/src/cas/delete_path.rs`. `write_path.rs` carries a
`BlockWriteGuard` with a `Drop` impl (`write_path.rs:64-68`) that reports
dropped blocks to metrics when a write was left `Pending` -- an
observability hook for the leak case the contract permits.

Test coverage of the contract is real and reachable in the names surfaced by
cmm: `do_test_store_object_refcount`,
`do_test_store_and_delete_object_with_refcount_same_blocks_diffkey`.

## Inline data

Small objects can be stored inside their metadata record rather than as
separate block files, controlled by `inlined_metadata_size` (threaded through
`FjallStore::new` and `CasFS::new`; default at
`cas-storage/src/metastore/stores/fjall.rs:29`):

```rust
const DEFAULT_INLINED_METADATA_SIZE: usize = 1; // setting very low will practically disable it by default
```

So inlining is effectively off unless a caller opts in. `ObjectData`
(`cas-storage/src/metastore/object.rs`) is the enum carrying either inline
bytes, a single-part block list, or the multipart shape.

## Durability

`Durability` (`cas-storage/src/metastore/traits.rs`, mapped in
`stores/fjall.rs`) has three levels, translated onto fjall's `PersistMode`:

| `Durability` | fjall `PersistMode` |
|--------------|---------------------|
| `Buffer` | `Buffer` |
| `Fsync` | `SyncAll` |
| `Fdatasync` | `SyncData` |

Default is `Fsync`, and the `s3cas` CLI default is `fsync` -- the strongest
mode.

The mapping follows the POSIX names: `fsync` flushes data and metadata
(strongest), `fdatasync` flushes data only (weaker, faster). It was originally
crossed -- `Fsync` selected `SyncData` and `Fdatasync` selected `SyncAll`,
with `fdatasync` as the default -- which was flagged as finding H12 and fixed
on 2026-07-30 by swapping the mapping and moving the default to `Fsync`, so
the default persist behavior is unchanged while the names now tell the truth.

## Transactional model

`FjallStore::begin_transaction` (`stores/fjall.rs:119-131`) takes a fjall
single-writer write transaction and extends its lifetime to `'static` via
`mem::transmute`, then hands it to a `FjallTransaction` that also holds an
`Arc<FjallStore>` clone.

The lifetime extension is sound in practice because the `Arc<FjallStore>` clone
keeps the underlying `Arc<SingleWriterTxDatabase>` alive, and `FjallTransaction`
declares `tx` before `store` so the transaction drops first. But it is asserted,
not proven, and it is accompanied by `unsafe impl Send`/`unsafe impl Sync` with
no safety argument. See findings H2 and H5.

`FjallStore::num_keys` is `unimplemented!()` (`stores/fjall.rs:132-135`) --
fjall's transactional keyspace does not expose a key count. This is a
**reachable panic**, not a placeholder; see finding H1.
