# Storage Model (As-Built)

## Content addressing

Objects are split into fixed 1 MiB blocks. From `cas-storage/src/cas/fs.rs:17`:

```rust
pub const BLOCK_SIZE: usize = 1 << 20; // Supposedly 1 MiB
```

("Supposedly" is upstream's comment, not a note added here.)

Each block is hashed with **BLAKE3**. The digest is the `BlockId`
(`cas-storage/src/metastore/block.rs`), which serves as both the key in the
block metadata tree and the basis for the on-disk path. There is exactly one
site that produces a block address, `cas-storage/src/cas/write_path.rs`, and it
takes the hasher from the store rather than calling a hash function directly.

The address width is a per-store choice, fixed at creation and recorded in the
store header:

| Width | `Hasher` variant | What it is | When |
|-------|------------------|------------|------|
| 32 bytes | `Blake3W32` | BLAKE3's native 256-bit output | Default. Required for untrusted multi-tenancy |
| 16 bytes | `Blake3W16` | BLAKE3 truncated to its leading 16 bytes | Trusted-tenant deployments that want smaller metadata |

`Hasher` (`cas-storage/src/hasher.rs`) is a concrete enum, not a trait: a block
hash is one-shot over a fully buffered chunk, so there is no streaming state to
abstract, and an exhaustive match makes every width-sensitive site fail to
compile when a variant is added. Both widths report the same `algo_id` (1,
"blake3") because the width is a separate header field, not a separate
algorithm. Truncation is a prefix: a `Blake3W16` address is byte-for-byte the
first 16 bytes of the `Blake3W32` address of the same block, and a test pins
that.

The width is a metadata-size decision, not a throughput one. The
`store_by_hash_width` group in `benches/casfs_benchmark.rs` runs the same
object sizes at both widths and shows no measurable write-path difference;
what 16 buys is 16 fewer bytes per block id in every record that lists one.

MD5 survives in exactly one role: the S3 ETag, which is a protocol obligation
over whole objects and never a block address. It is computed inside
`cas-storage` (`cas/write_path.rs`, streaming alongside the block hashing),
which is why `md-5` is still a `cas-storage` dependency. The type system keeps
the two apart -- `ContentHash` (`cas-storage/src/metastore/content_hash.rs`) is
a fixed 16-byte newtype with its own `CONTENT_HASH_SIZE`, so the serialization
offsets for an ETag cannot follow the block width. The multipart ETag
(`s3cas/src/s3fs.rs::calculate_multipart_hash`) is the S3 convention: MD5 over
the concatenated per-part MD5s, rendered `{hex}-{N}` with N the part count.

## Store header (QSST)

Every fjall database this codebase creates carries a 32-byte header
(`cas-storage/src/metastore/store_header.rs`) in a `_STORE_HEADER` partition
under a single fixed key, written through `MetaStore::open_or_create`:

```
[ magic: "QSST"           ]  4 bytes
[ version: u16 LE         ]  2 bytes   currently 1
[ hash_algo: u8           ]  1 byte    blake3 = 1
[ hash_width: u8          ]  1 byte    16 or 32
[ created_at: u64 LE      ]  8 bytes   unix seconds
[ reserved                ]  16 bytes  written zeroed
```

The header lives at `MetaStore` level rather than in the CAS layer, so a store
that never addresses a block gets format versioning too: the shared block DB,
every namespace DB (`CasFS::single_namespace` therefore creates two headered
databases), and respd's key-value DB all have one. The hash fields are written
everywhere and consulted only by `SharedBlockStore`, which decodes them into
its `Hasher` at open.

Semantics:

- **Refusal, not migration.** A header that is missing on a non-empty store,
  has bad magic, names a version this build does not know, or names a hash
  this build does not have is refused at open with an operator-readable error
  naming the store path. There is no fallback: the blocks are addressed by
  what the header says, so guessing wrong corrupts rather than degrades.
- Create-versus-open is decided by inspecting the db directory *before* fjall
  opens it, since opening is what would create it.
- A byte-for-byte sidecar copy, `store_header.bin`, is written next to the db
  directory at creation. Recovery from it is a manual operation.
- The 16 reserved bytes round-trip untouched and are not rejected when a
  future version puts something there. Changing the meaning of an existing
  field requires a version bump instead. This is where a salt or key id would
  go if the question reopens (ADR 0002 resolved it as not implemented).
- Bucket names starting with `_` are refused at creation, so a bucket can
  never collide with `_STORE_HEADER`, `_BLOCKS`, `_PATHS` or
  `_MULTIPART_PARTS`.

`s3cas inspect header` prints magic, version, algo, width and created_at for
every metadata database under a `--meta-root`.

What a new store's header says comes from `[store.hash]` in
`qss_storage.toml` (`algo`, `width`; defaults `blake3` and 32). That table is
read at creation only: on open the header on disk wins, and a config that
disagrees with the store it opened gets a warning at startup rather than a
silent reinterpretation. Changing the block hash of a deployment means
creating a new store.

## Verify on read

Off by default, enabled with `store.verify_on_read` in `qss_storage.toml`.
When on, `BlockStream::verified` (`cas-storage/src/cas/block_stream.rs`)
buffers each whole block, re-hashes it with the store's hasher, and compares
against the address it was fetched by; a mismatch surfaces as a
`BlockCorruption` error naming the block and the file rather than serving the
bytes. With the flag off the stream is byte-for-byte the old streaming path.

Two limits worth stating plainly:

- **Whole blocks only.** A range request cannot be checked against a
  whole-block address, so ranged reads are unverified whatever the flag says.
- **It is bitrot detection, not a substitution defence.** The defence against
  a manufactured dedup collision is the 32-byte address itself.

Verification costs a full block buffer plus a hash per block, which is why it
is opt-in. `s3cas check` does the same comparison over every block file
offline and keeps the flag off for itself whatever the config says, so that it
can report every bad block instead of stopping at a read-path error on the
first one.

## Keyspaces

Three fjall trees, named at `cas-storage/src/metastore/meta_store.rs:22-24`:

| Tree | Constant | Holds |
|------|----------|-------|
| `_BUCKETS` | `DEFAULT_BUCKET_TREE` | `BucketMeta` per bucket |
| `_BLOCKS` | `DEFAULT_BLOCK_TREE` | `Block` records, keyed by `BlockId`, carrying the refcount |
| `_PATHS` | `DEFAULT_PATH_TREE` | block path allocation |

Two more reserved trees sit alongside them: `_MULTIPART_PARTS`, opened by
`SharedBlockStore` for in-flight multipart uploads, and `_STORE_HEADER`, which
holds the single QSST header record (see below). The leading underscore is what
marks a tree as internal, which is why bucket names starting with `_` are
refused at creation.

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

There is no version or magic byte in an individual record; versioning is per
store, in the QSST header described above, which was the next step of the same
ADR 0002 pass. A store written before format v1 has no header at all and is
refused at open with an explicit "predates the QSST format, no migration
exists" error, rather than decoding into garbage -- which is the intended
outcome, since there is no backward compatibility with the pre-v1 layout.

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
`FjallStore::new` and `CasFS::new`, and settable as `store.inline_metadata_size`
in `qss_storage.toml`; default at
`cas-storage/src/metastore/stores/fjall_common.rs:33`):

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

`FjallStore::begin_transaction` (`stores/fjall.rs`, around the `transmute` at
line 112) takes a fjall single-writer write transaction and extends its
lifetime to `'static` via `mem::transmute`, then hands it to a
`FjallTransaction` that also holds an `Arc<FjallStore>` clone.

The lifetime extension is sound because the `Arc<FjallStore>` clone keeps the
underlying `Arc<SingleWriterTxDatabase>` alive, and `FjallTransaction` declares
`tx` before `store` so the transaction drops first. Both facts are now written
down as a `SAFETY` argument with the field order flagged as load-bearing
(finding H2). `unsafe impl Sync` was deleted -- the auto impl suffices, and a
`const _` static assertion now guards it -- while `unsafe impl Send` is
required (`SingleWriterWriteTx` holds a `MutexGuard`) and carries an honest
argument, including the part that rests on no caller holding a transaction
across an `.await`.

`FjallStore::num_keys` counts through a read transaction like `len` does
(`stores/fjall.rs:81`). It used to be `unimplemented!()`, a reachable panic on
the default `--metadata-db fjall` path of `s3cas inspect num-keys`; the panic
message claiming fjall could not count was simply wrong. See finding H1.
