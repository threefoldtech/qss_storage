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
[ version: u16 LE         ]  2 bytes   currently 2
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
  never collide with `_STORE_HEADER`, `_BLOCKS`, `_MULTIPART_PARTS` or
  `_UPLOADS`.
- Version history: v1 was ADR 0002 (BLAKE3 addressing, u64 fields, this
  header). v2 is ADR 0006: block records store a fanout depth instead of
  allocated path bytes and the `_PATHS` tree no longer exists. A v1 store
  is refused at open; no migration exists (no deployed v1 store carried
  data).

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

Two fjall trees, named at `cas-storage/src/metastore/meta_store.rs:22-23`:

| Tree | Constant | Holds |
|------|----------|-------|
| `_BUCKETS` | `DEFAULT_BUCKET_TREE` | `BucketMeta` per bucket |
| `_BLOCKS` | `DEFAULT_BLOCK_TREE` | `Block` records, keyed by `BlockId`, carrying the refcount and fanout depth |

(`_PATHS`, the block path allocator, died with ADR 0006: block file paths
are derived from the id and the recorded depth, so there is nothing to
allocate.)

Three more reserved trees sit alongside them, all opened by
`SharedBlockStore`: `_MULTIPART_PARTS` (`MULTIPART_PARTS_TREE`,
`meta_store.rs:40`) holding one record per uploaded part, `_UPLOADS`
(`UPLOADS_TREE`, `meta_store.rs:49`) holding one record per in-flight
multipart upload (ADR 0003), and `_STORE_HEADER`, which holds the single
QSST header record (see below). The leading underscore is what marks a tree
as internal, which is why bucket names starting with `_` are refused at
creation.

Object metadata lives in a per-bucket tree, opened by bucket name. Multipart
state lives in the two trees above (`cas-storage/src/cas/multipart.rs`,
`MultiPartTree`; `cas-storage/src/cas/uploads.rs` for the upload side).
Both are in the shared blocks database, not the namespace one, so a single
transaction spans them -- which is what lets `complete_multipart_upload`
take an upload record and its part records together.

Keys in both are length-prefixed byte strings, not joined strings:

```
_UPLOADS:         bucket_len u64 | bucket | key_len u64 | key | upload_id
_MULTIPART_PARTS: bucket_len u64 | bucket | key_len u64 | key |
                  upload_len u64 | upload_id | part_number u64 BE
```

Nothing parses them back: point reads rebuild the key they wrote, and scans
decode record values. The part key's trailing big-endian part number makes
all parts of one upload share an exact byte prefix, so per-upload
enumeration is a prefix scan (`MultiPartTree::parts_of`). Part records
written before ADR 0003 used `{bucket}-{key}-{upload_id}-{part_number}`,
which is ambiguous and unreachable by both the new point reads and the
prefix scan; the value-driven orphan sweep collects them.

In `SharedBlockStore` mode, `_BLOCKS` and the block files are shared across all
namespaces while each namespace keeps its own bucket and object trees.

## On-disk record formats (v2)

Serialization is hand-rolled little-endian byte packing, not a serde format.
Each type has a `to_vec` and a `TryFrom<&[u8]>`. All length and count fields
are `u64`, independent of the host pointer width; the shared cursor and the
id-list helpers live in `cas-storage/src/metastore/codec.rs`.

Every record is length-exact: a short buffer decodes to `FsError::Truncated`,
a long one to `FsError::TrailingBytes`. Byte-for-byte golden vectors for all
five records are pinned in the `mod tests` of each of the files below.

### Block (`cas-storage/src/metastore/block.rs`)

```
[ size: u64 LE            ]  8 bytes
[ depth: u8               ]  1 byte
[ rc: u64 LE              ]  8 bytes
```

`depth` is the fanout depth the block file was placed at, `1..=width(id)`.
The file path is derived, never stored: `block_disk_path(id, depth, root)`
yields `blocks/<hex b0>/.../<hex b(depth-1)>/<full-hex id>` -- directory
names are single hex bytes of the id's prefix, the filename is the full
id, so a file's name identifies its block at any depth and the depth
choice is pure placement (ADR 0006). Format v1 stored allocated path
bytes here (`path_len u8 | path`).

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

### UploadRecord (`cas-storage/src/metastore/upload_record.rs`)

```
[ created_at: i64 LE      ]  8 bytes
[ bucket_len: u64 LE      ]  8 bytes
[ bucket: UTF-8 bytes     ]
[ key_len: u64 LE         ]  8 bytes
[ key: UTF-8 bytes        ]
[ upload_len: u64 LE      ]  8 bytes
[ upload_id: UTF-8 bytes  ]
```

One row of `_UPLOADS` per in-flight multipart upload (ADR 0003). The
record's existence is the upload's: `upload_part` refuses an id that has
none, and `complete_multipart_upload` and `abort_multipart_upload` both
begin by claiming it -- `Transaction::take_upload`, an atomic read+remove
on the shared database, so exactly one of them wins and the loser answers
`NoSuchUpload`. Complete's claim takes the named part records in the same
transaction. The record lives in the metastore layer rather than beside
the upload code in `cas` because the transaction decodes it, and the
metastore names no type from above it. `created_at` is a wall-clock Unix
timestamp; the stale-upload GC and fsck both age against it. Added without
a store-header bump: a new tree with new keys changes no existing record.

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
- ANY reuse of an existing block -> rc + 1 (same key or different key;
  the same-key skip was removed by ADR 0006 -- its under-count was the
  loss trace in `docs/arch/key-has-block-skip.md`)
- object overwritten -> rc - 1 per occurrence of the object it replaced,
  through the same `release_blocks` loop a DELETE uses, after the new
  record commits (ADR 0008). A same-content re-PUT therefore nets to no
  change: bumped by the write, dropped by the release
- object deleted -> rc - 1 per block occurrence
- part record reaped -> rc - 1 per block occurrence: a multipart abort,
  the stale-upload GC, or fsck's `reap_orphan_part`, all through
  `release_blocks` (`delete_path.rs`), the same striped loop
  `delete_object` uses
- multipart upload completed -> no rc change at all: the object record
  inherits the references its part records held
- rc reaches 0 -> record removed and file unlinked under one stripe hold

The write path lives in `cas-storage/src/cas/write_path.rs`, the delete path in
`cas-storage/src/cas/delete_path.rs`. `write_path.rs` carries a
`BlockWriteGuard` with a `Drop` impl (`write_path.rs:64-68`) that reports
dropped blocks to metrics when a write was left `Pending` -- an
observability hook for the leak case the contract permits.

Test coverage of the contract is real and reachable:
`do_test_store_object_refcount`,
`do_test_store_and_delete_object_with_refcount_same_blocks_diffkey`,
`test_double_delete_same_key_is_idempotent`.

## Inline data

Small objects can be stored inside their metadata record rather than as
separate block files, controlled by `inlined_metadata_size` (threaded through
`FjallStore::new` and `CasFS::new`, and settable as `store.inline_metadata_size`
in `qss_storage.toml`; default in
`cas-storage/src/metastore/stores/fjall.rs`):

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

## Block write and delete protocol (ADR 0006, as built 2026-07-31)

The write and delete paths were redesigned by ADR 0006; the full account
(defects, decisions, rejected alternatives) is in
`docs/adr/0006-block-write-protocol.md`. The shape as built:

- **Disk layout.** `blocks/<hex b0>/.../<hex b(d-1)>/<full-hex id>` at an
  adaptive depth d chosen per block at write time (shallowest fanout dir
  under ~4096 entries, `cas-storage/src/cas/placement.rs`); temp files
  live in `blocks/.tmp/<hex id>-<nonce>` and are purged once at store
  open. The blocks root, stripe set, placement state, and atomic writer
  all live on `SharedBlockStore` -- one per store, shared by every
  namespace.
- **Striped locking.** 1024 async mutexes by default, indexed by the
  id's first two bytes (`cas-storage/src/cas/stripes.rs`). Every
  `_BLOCKS` mutation -- insert, dedup bump, decrement, removal -- runs
  under the block's stripe; `BlockTree` has no mutators.
- **PUT** (`write_one_block`, `cas-storage/src/cas/write_path.rs`): take
  the stripe, then inside ONE `spawn_blocking` closure that owns the
  guard: a transactional dedup RMW (hit: bump rc, done); on miss, choose
  the depth (orphan probe first: a file named `<id>` on the id's dir
  chain is healed in place by the rename), write temp + fsync + rename +
  fsync dirs (`cas-storage/src/cas/block_disk.rs`, gated by
  `Durability`; `Buffer` skips all fsyncs), then a second transaction
  inserts the record. The record commits only after the file is durable
  at its final path, so a crash never leaves a record without a complete
  file.
- **DELETE** (`cas-storage/src/cas/delete_path.rs`): the object record
  is read and removed in one namespace-DB transaction (idempotent,
  defeats double-DELETE double-decrements), then each block occurrence
  is decremented under its stripe in its own blocking closure; at rc==0
  the record removal commits first and the unlink follows inside the
  same stripe hold.
- **Cancellation.** Guards move into the blocking closures, which run to
  completion even when the awaiting request future is dropped; residue
  is a completed bump or a record+file without an object -- leak class,
  never a torn state.
- **Observability.** `s3_data_block_disk_ops_inflight` gauges submitted
  vs completed blocking closures (blocking-pool queue depth); the
  `BlockWriteGuard` pending/written/error/dropped counters survive from
  the previous design.
