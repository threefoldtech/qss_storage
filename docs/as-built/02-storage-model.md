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

## On-disk record formats (v1)

Serialization is hand-rolled little-endian byte packing, not a serde format.
Each type has a `to_vec` and a `TryFrom<&[u8]>`. All length and count fields
are `u64`, independent of the host pointer width; the shared cursor and the
id-list helpers live in `cas-storage/src/metastore/codec.rs`.

Every record is length-exact: a short buffer decodes to `FsError::Truncated`,
a long one to `FsError::TrailingBytes`. Byte-for-byte golden vectors for all
four records are pinned in the `mod tests` of each of the files below.

### Block (`cas-storage/src/metastore/block.rs`)

```
[ size: u64 LE            ]  8 bytes
[ path_len: u8            ]  1 byte
[ path: bytes             ]  path_len bytes
[ rc: u64 LE              ]  8 bytes
```

The path length stays a single byte: a path is a prefix of a block hash, so it
is at most one full hash width (32) < 256 bytes.

### BucketMeta (`cas-storage/src/metastore/bucket_meta.rs`)

```
[ ctime: i64 LE           ]  8 bytes
[ name_len: u64 LE        ]  8 bytes
[ name: UTF-8 bytes       ]  name_len bytes
```

### Object (`cas-storage/src/metastore/object.rs`)

```
[ type: u8                ]  1 byte    0 Single, 1 Multipart, 2 Inline
[ size: u64 LE            ]  8 bytes
[ ctime: i64 LE           ]  8 bytes
[ hash: bytes             ]  CONTENT_HASH_SIZE (16) bytes
```

then, per type:

```
Inline:      [ data_len: u64 LE ] [ data ]
SinglePart:  [ id_width: u8 ] [ count: u64 LE ] [ ids: count * id_width ]
MultiPart:   [ parts: u64 LE ] [ id_width: u8 ] [ count: u64 LE ] [ ids ]
```

The shortest valid record is an Inline object with no data, 41 bytes. That is
also `Object::minimum_inline_metadata_size()`, which
`MetaStore::max_inlined_data_length` subtracts from the configured inline
budget.

### MultiPart part record (`cas-storage/src/cas/multipart.rs`)

```
[ size: u64 LE            ]  8 bytes
[ part_number: i64 LE     ]  8 bytes
[ bucket_len: u64 LE      ]  8 bytes
[ bucket: UTF-8 bytes     ]
[ key_len: u64 LE         ]  8 bytes
[ key: UTF-8 bytes        ]
[ upload_len: u64 LE      ]  8 bytes
[ upload_id: UTF-8 bytes  ]
[ hash: bytes             ]  CONTENT_HASH_SIZE (16) bytes
[ id_width: u8            ]  1 byte
[ count: u64 LE           ]  8 bytes
[ ids: count * id_width   ]
```

## Block-id widths in records

Records that carry a block-id list write a self-describing width byte, so
`TryFrom<&[u8]>` stays context-free: a decoder never has to be told which width
the store that wrote the record uses.

- `id_width` is 16 or 32; anything else is `FsError::InvalidIdWidth`.
- `id_width` 0 is legal only when `count` is 0, which is how an empty list is
  written.
- All ids in one record share a width (they come from one store). The
  serializer takes the width from the first id and `debug_assert`s the rest.

The list length is derived exactly as `count * id_width`, so trailing garbage
is reported rather than absorbed as extra block ids -- which is what the old
`chunks_exact`-over-the-remainder loop did.

## Pointer-width dependence (resolved)

Format v1 removed `PTR_SIZE` (`metastore/constants.rs`, now deleted) from the
records: every length, count and refcount field is a fixed `u64`, so a store
written on a 64-bit host has the same bytes as one written on a 32-bit host.

`usize` is still the in-memory type for `Block.size`/`rc`, `MultiPart.size` and
`ObjectData::MultiPart.parts`; the conversion happens at the decode boundary
with `try_into`, and a value that does not fit the host `usize` surfaces as
`FsError::LengthOverflow` rather than a panic. Derived offsets are computed
with checked arithmetic for the same reason.

There is still no version or magic byte in a record; that arrives with the
store header (ADR 0002 implementation, component 5). Until then, a store
written before format v1 reads as a decode error, which is the intended
outcome -- there is no backward compatibility with the pre-v1 layout.

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
