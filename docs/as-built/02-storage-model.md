# Storage Model (As-Built)

## Content addressing

Objects are split into fixed 1 MiB blocks. From `cas-storage/src/cas/fs.rs:18`:

```rust
pub const BLOCK_SIZE: usize = 1 << 20; // Supposedly 1 MiB
```

("Supposedly" is upstream's comment, still on that line, not a note added
here.)

Each block is hashed with **BLAKE3**. The digest is the `BlockId`
(`cas-storage/src/metastore/block.rs`), which serves as both the key in the
block metadata tree and the basis for the on-disk path. There is exactly one
production site that produces a block address, `write_path.rs:638`, and it
takes the hasher from the store rather than calling a hash function directly.
The only other production callers of the hasher re-compute an address to
compare it -- `block_stream.rs:163` for `verify_on_read`, `check.rs:117` for
`s3cas check` -- and both take the same store hasher.

A `BlockId` is never the hash of a whole object. That is a separate address
space with a separate type -- see `ContentHash` below for the S3 ETag, and
the content-addressed namespace key (ADR 0014), which is a BLAKE3-256 over
the whole value and is fixed at 32 bytes whatever the store's block width is.

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
(`s3cas/src/api.rs::calculate_multipart_hash`) is the S3 convention: MD5 over
the concatenated per-part MD5s, rendered `{hex}-{N}` with N the part count.

## Store header (QSST)

Every fjall database this codebase creates carries a 32-byte header
(`cas-storage/src/metastore/store_header.rs`) in a `_STORE_HEADER` partition
under a single fixed key, written through `MetaStore::open_or_create`:

```
[ magic: "QSST"           ]  4 bytes
[ version: u16 LE         ]  2 bytes   3 at creation; 4 once raised
[ hash_algo: u8           ]  1 byte    blake3 = 1
[ hash_width: u8          ]  1 byte    16 or 32
[ created_at: u64 LE      ]  8 bytes   unix seconds
[ store_id                ]  16 bytes  v4 UUID; all-zero means absent
```

The header lives at `MetaStore` level rather than in the CAS layer, so a store
that never addresses a block gets format versioning too: the shared block DB,
every namespace DB (`CasFS::single_namespace` therefore creates two headered
databases), and respcas's key-value DB all have one. The hash fields are written
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
- The last 16 bytes were reserved until ADR 0012 spent them on the `store_id`
  (see below). Whatever pattern they hold round-trips untouched, so a build
  that only passes a header through neither rejects nor drops an id it did not
  mint; there are no spare bytes left, so a new field means a version bump. A
  salt or key id, if that question reopens (ADR 0002 resolved it as not
  implemented), would need one.
- Bucket names starting with `_` are refused at creation, so a bucket can
  never collide with `_STORE_HEADER`, `_BLOCKS`, `_MULTIPART_PARTS` or
  `_UPLOADS`.
- **Created at, versus opened at.** `STORE_HEADER_VERSION` (3) is what a new
  store is written with; `SUPPORTED_STORE_HEADER_VERSIONS` (`{3, 4}`) is what
  this build will open. The two differ since ADR 0014 -- see the version
  history below.
- Version history:
  - **v1**, ADR 0002: BLAKE3 addressing, u64 on-disk fields, this header.
  - **v2**, ADR 0006: block records store a fanout depth instead of allocated
    path bytes; the `_PATHS` tree no longer exists.
  - **v3**, ADR 0005: block records gain a trailing flags byte carrying the
    degraded bit. A v2 record is one byte short of a v3 one and fails the
    exact-length check.
  - **v4**, ADR 0014: a respcas store that holds a content-addressed
    namespace. Unlike every version before it, v4 is RAISED on a live store
    rather than written at creation: the first `NSSET <ns> key_mode cas`
    calls `store_header::raise_version` BEFORE writing namespace metadata
    carrying a msgpack variant an older build cannot decode. A store that
    never uses the feature stays at v3 and stays openable by pre-0014
    builds. There is no "minor version" field -- the version is one
    exact-match `u16`, and the supported-versions list is the gate.

  An older version is refused at open, not migrated; no deployed v1 or v2
  store carried data.

`s3cas inspect header` prints magic, version, algo, width and created_at for
every metadata database under a `--meta-root`.

What a new store's header says comes from `[store.hash]` in
`qss_storage.toml` (`algo`, `width`; defaults `blake3` and 32). That table is
read at creation only: on open the header on disk wins, and a config that
disagrees with the store it opened gets a warning at startup rather than a
silent reinterpretation. Changing the block hash of a deployment means
creating a new store.

## Tiered store roots and pairing identity (ADR 0012, as built 2026-08-01)

A store has two roots and they may be two disks. `--meta-root` holds every
database; `--fs-root` holds every block file. Nothing about the split is new
plumbing -- the paths already flowed separately -- but it is now a declared,
checked store shape rather than an accident of two flags:

```
--meta-root (NVMe)                     --fs-root (HDD)
  db/                namespace DB        blocks/aa/../<hash>   block files
  blocks/.db/        blocks DB           blocks/.tmp/          staging (same fs)
  store_header.bin   sidecars            blocks/.store-id      pairing marker
  blocks/store_header.bin

journal persists, compactions,         1 MiB whole-file writes, fdatasync
dedup point reads, record walks        waves, fanout dirsyncs, block reads
```

Running both roots at the same path is unchanged and still the default; it
simply puts the databases inside the blocks root, where the scrub knows to
skip them.

**Placement is CLI-only.** There is no toml key for either root (the config
describes the store, the invocation places it) and no third `--blocks-db-path`:
the blocks DB follows the meta root. The `.tmp` same-filesystem rule is
unchanged and load-bearing -- staging lives under the blocks root, so ADR
0006's rename atomicity never crosses a device.

**Pairing identity.** Each store mints a `store_id` (v4 UUID) at creation; it
lives in the header (record and sidecar) and, as lowercase hex, in
`<fs_root>/blocks/.store-id`. The marker is written by the block writer's own
temp+fsync+rename protocol, so it is never half-written.

`SharedBlockStore::new` compares the two after the metastore opens, before any
tree is touched:

| header | marker | outcome |
|--------|--------|---------|
| id | same id | opens |
| id | different id | **refused**: both ids and both paths in the message, no override |
| id | absent | marker written (a crash between the two adoption writes) |
| id | unparseable | marker rewritten; damage names no store |
| absent | absent | adopted: id minted, header first, then marker |
| absent | id | the root's id is taken into the header |

Write order is header first, always: that is what makes the half-adopted state
repairable without judgement. Old stores are adopted, not refused -- deliberately
unlike the `blocks/.db` migration refusal, which protected against SHADOWING
live records with an empty database. Adoption writes two small identity
artifacts and shadows nothing.

The store-level id covers the namespace databases too: they are opened through
the same paired root, so there is no per-namespace pairing. (Every database
still carries an id of its own in its header; only the blocks DB's is ever
compared against a marker.)

respcas's `--data-dir` is in scope since ADR 0014, and it reaches the check
by the ordinary route: `Storage::new` builds a `SharedBlockStore` over
`<data_dir>/blocks` for both halves, so the pairing comparison runs on a
respcas store exactly as it does on an s3cas one. respcas has no meta-root /
fs-root split of its own -- the two paths it passes are the same directory,
which is the shape the tools default to.

**Recovery** is one verb, in the tool that can audit the result:

```
qss-storage-fsck --re-pair --meta-root /nvme/store --fs-root /hdd/store
```

It rewrites the marker to match the database's header, prints the id it wrote
and the one it replaced, and runs no passes. Both roots must be spelled out.
There is no daemon override flag, on purpose: one would end up in a unit file
and defeat the check forever. A store whose header has no id yet is refused by
the verb (open it once; adoption mints one).

**fsck's ordering.** The pairing check is the first thing that happens in any
run, because it happens during the store open -- a mispaired store never
reaches a walker, and cannot be "repaired" toward either side's fiction. It
surfaces as could-not-run (exit 3).

**Scrub's foreign-file table** gains `.store-id` beside `.db`, `.tmp` and
`.quarantine` (`cas-storage/src/scrub/disk.rs::is_the_stores_own`). Those four
names are skipped at the top of the blocks root and nowhere else; one level
down, an entry by any of them is foreign like anything else that is not
block-shaped. Old builds reading a new store's blocks root will report
`.store-id` as a foreign file: harmless, and reported rather than acted on.

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

Two fjall trees, named at `cas-storage/src/metastore/meta_store.rs:24` and
`:31`:

| Tree | Constant | Holds |
|------|----------|-------|
| `_BUCKETS` | `DEFAULT_BUCKET_TREE` | one record per bucket, keyed by the bucket NAME |
| `_BLOCKS` | `DEFAULT_BLOCK_TREE` | `Block` records, keyed by `BlockId`, carrying the refcount, fanout depth and flags |

(`_PATHS`, the block path allocator, died with ADR 0006: block file paths
are derived from the id and the recorded depth, so there is nothing to
allocate.)

The `_BUCKETS` VALUE is not one type. s3cas writes a `BucketMeta`; respcas
writes its own msgpack `NamespaceMeta` there. The key is the name in both
cases, which is why `MetaStore::list_bucket_names` (`meta_store.rs:341`)
exists alongside `list_buckets`: anything that only wants to know which
buckets exist reads the keys, and only the S3 listing decodes the record for
its creation time. ADR 0014 found this the hard way -- fsck's
`bucket_integrity` pass decoded every value as `BucketMeta` and therefore
errored on a healthy respcas store.

Three more reserved trees sit alongside them, all opened by
`SharedBlockStore`: `_MULTIPART_PARTS` (`MULTIPART_PARTS_TREE`,
`meta_store.rs:40`) holding one record per uploaded part, `_UPLOADS`
(`UPLOADS_TREE`, `meta_store.rs:49`) holding one record per in-flight
multipart upload (ADR 0003), and `_STORE_HEADER`, which holds the single
QSST header record (see above). The leading underscore is what marks a tree
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

## On-disk record formats (v3)

Serialization is hand-rolled little-endian byte packing, not a serde format.
Each type has a `to_vec` and a `TryFrom<&[u8]>`. All length and count fields
are `u64`, independent of the host pointer width; the shared cursor and the
id-list helpers live in `cas-storage/src/metastore/codec.rs`.

Every record is length-exact: a short buffer decodes to `FsError::Truncated`,
a long one to `FsError::TrailingBytes`. Byte-for-byte golden vectors for all
five records are pinned in the `mod tests` of each of the files below.

(These are the records of the CAS layer. respcas's `NamespaceMeta` is not one
of them: it is msgpack in the `_BUCKETS` value, with its own compatibility
tests in `respcas/src/storage.rs`.)

### Block (`cas-storage/src/metastore/block.rs`)

```
[ size: u64 LE            ]  8 bytes
[ depth: u8               ]  1 byte
[ rc: u64 LE              ]  8 bytes
[ flags: u8               ]  1 byte
```

`depth` is the fanout depth the block file was placed at, `1..=width(id)`.
The file path is derived, never stored: `block_disk_path(id, depth, root)`
yields `blocks/<hex b0>/.../<hex b(depth-1)>/<full-hex id>` -- directory
names are single hex bytes of the id's prefix, the filename is the full
id, so a file's name identifies its block at any depth and the depth
choice is pure placement (ADR 0006). Format v1 stored allocated path
bytes here (`path_len u8 | path`).

`flags` is ADR 0005's addition (store header v3): bit 0 is `FLAG_DEGRADED`,
set when the block's data is gone or corrupt. The record survives a degraded
block so the holders' accounting stays intact, and a degraded record is
absent as far as dedup is concerned. The remaining seven bits are reserved.
A v2 record is one byte short of a v3 one, which is what makes the
exact-length check refuse it rather than misread it.

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
- record cloned by reference into another bucket -> rc + 1 per block
  occurrence, taken BEFORE the destination record is written (ADR 0014,
  `cas-storage/src/cas/clone_path.rs`). Every holder accounts for itself,
  so a later DELETE in one namespace cannot strand another
- rc reaches 0 -> record removed and file unlinked under one stripe hold

The write path lives in `cas-storage/src/cas/write_path.rs`, the delete path in
`cas-storage/src/cas/delete_path.rs`. `write_path.rs` carries a
`BlockWriteGuard` with a `Drop` impl (`write_path.rs:118-124`) that reports
dropped blocks to metrics when a write was left `Pending` -- an
observability hook for the leak case the contract permits.

Test coverage of the contract is real and reachable:
`do_test_store_object_refcount`,
`do_test_store_and_delete_object_with_refcount_same_blocks_diffkey`,
`test_double_delete_same_key_is_idempotent`.

## Inline data

Small objects can be stored inside their metadata record rather than as
separate block files, controlled by `inlined_metadata_size` (carried in
`StoreOptions`, threaded through `FjallStore::new` and the `CasFS`
constructors, and settable as `store.inline_metadata_size` in
`qss_storage.toml`; default at
`cas-storage/src/metastore/stores/fjall.rs:38`):

```rust
pub const DEFAULT_INLINED_METADATA_SIZE: usize = 1;
```

So inlining is effectively off unless a caller opts in. `ObjectData`
(`cas-storage/src/metastore/object.rs`) is the enum carrying either inline
bytes, a single-part block list, or the multipart shape.

`MetaStore::max_inlined_data_length` is the configured budget minus
`Object::minimum_inline_metadata_size()` (41 bytes), which is the number the
write paths compare a value's length against. respcas defaults the budget to
1 byte (`config::DEFAULT_RESP_INLINE_METADATA_SIZE`), so a respcas
deployment that configures nothing keeps inlining everything it used to and
sends nothing down the block path until an operator raises the threshold.

## Durability

`Durability` (`cas-storage/src/metastore/traits.rs:222`, mapped in
`stores/fjall.rs`) has two levels, translated onto fjall's `PersistMode`:

| `Durability` | fjall `PersistMode` |
|--------------|---------------------|
| `Buffer` | `Buffer` |
| `Fsync` | `SyncAll` |

Default is `Fsync`, and the `s3cas` CLI default is `fsync` -- the stronger of
the two.

There were three levels once. The middle one, `fdatasync`, mapped to fjall's
`SyncData`; ADR 0010 removed it with no compatibility alias, and the parser
refuses the name with the two valid ones in the message. The reasoning: after
the sync boundary moved from the block to the ack, the two syscalls' cost
difference is paid once per request instead of twice per MiB, and on an
append-only journal even fdatasync must flush the size metadata -- so the two
levels became indistinguishable in speed and in crash safety, and a knob
naming a dead tradeoff misleads whoever picks it.

The fdatasync SYSCALL did not go anywhere: it is what the batch uses on block
files, where the per-batch directory fsync carries the rename's durability.

## Transactional model

`FjallStore::begin_transaction` (`stores/fjall.rs`, around the `transmute` at
line 312) takes a fjall single-writer write transaction and extends its
lifetime to `'static` via `mem::transmute`, then hands it to a
`FjallTransaction` that also holds an `Arc<FjallStore>` clone.

The lifetime extension is sound because the `Arc<FjallStore>` clone keeps the
underlying `Arc<SingleWriterTxDatabase>` alive, and `FjallTransaction` declares
`tx` before `store` so the transaction drops first. Both facts are now written
down as a `SAFETY` argument with the field order flagged as load-bearing
(finding H2). `unsafe impl Sync` was deleted -- the auto impl suffices, and a
`const _` static assertion at `fjall.rs:655` now guards it -- while
`unsafe impl Send` (`fjall.rs:643`) is required (`SingleWriterWriteTx` holds a
`MutexGuard`) and carries an honest argument, including the part that rests on
no caller holding a transaction across an `.await`. That transmute and that
one `unsafe impl` are the only `unsafe` left in `cas-storage`.

`FjallStore::num_keys` (`stores/fjall.rs:324`) counts through a read
transaction like `len` does. It used to be `unimplemented!()`, a reachable
panic on the default `--metadata-db fjall` path of `s3cas inspect num-keys`;
the panic message claiming fjall could not count was simply wrong. See
finding H1.

### Ack durability classes (ADR 0013)

Not every write that answers a client goes through a transaction.
`CreateBucket`, `CreateMultipartUpload`, `UploadPart`'s ETag and respcas's
`SET` and `DEL` all ack on a bare `FjallTree::insert`/`remove`, which used to
return before anything was persisted. ADR 0013 gave each tree an
`AckPersist` class (`stores/fjall.rs:344`, set when the tree is opened):

| Class | Trees | What the ack promises |
|-------|-------|-----------------------|
| `Contract` | everything else, including respcas's namespace trees | persisted at the store's configured `Durability` before returning |
| `Recoverable` | `_MULTIPART_PARTS`, `_UPLOADS` | fjall's internal kernel-visible write only |

The `Recoverable` class is not an oversight: a power cut that takes a part
record is answered by the protocol as `InvalidPart` / `NoSuchUpload` -- loud,
retryable, and leak-class at worst, since the over-counted blocks are what
the ADR 0005 recount collects. Either way the bytes are the kernel's before
the ack, so a `kill -9` takes nothing; only a power cut can, and only in the
`Recoverable` class.

## Block write and delete protocol (ADR 0006, widened by 0010 and 0011)

The write and delete paths were redesigned by ADR 0006; the full account
(defects, decisions, rejected alternatives) is in
`docs/adr/0006-block-write-protocol.md`. ADR 0010 then moved the sync
boundary from the block to the ack and ADR 0011 added an optional station
that merges the closing step of concurrent requests. Nothing about the
ORDERING moved in either -- the shape as built:

- **Disk layout.** `blocks/<hex b0>/.../<hex b(d-1)>/<full-hex id>` at an
  adaptive depth d chosen per block at write time (shallowest fanout dir
  under `DEFAULT_MAX_DIR_ENTRIES` = 4096 entries,
  `cas-storage/src/cas/placement.rs`); temp files live in
  `blocks/.tmp/<hex id>-<nonce>` and are purged once at store open. The
  blocks root, stripe set, placement state, atomic writer and commit
  station all live on `SharedBlockStore` -- one per store, shared by every
  namespace.
- **Orphan healing.** Before applying the placement policy, `choose_depth`
  probes the id's directory chain for a file already named `<id>`. A hit
  means an orphan from an earlier attempt, and its depth is returned so the
  rename overwrites it in place rather than stranding it at one depth and
  writing a second copy at another.
- **Striped locking.** 1024 async mutexes by default
  (`DEFAULT_STRIPE_COUNT`, `store.stripe_count` to override), indexed by
  the id's first two bytes modulo the count
  (`cas-storage/src/cas/stripes.rs`). Every `_BLOCKS` mutation -- insert,
  dedup bump, decrement, removal -- runs under the block's stripe;
  `BlockTree` has no mutators.
- **PUT, at batch width** (`cas-storage/src/cas/write_path.rs`). As each
  1 MiB chunk arrives it is hashed, looked up for dedup, and (on a miss)
  written to a temp file; a hit holds its bytes rather than dropping them,
  because a concurrent DELETE can still take the record's last reference
  before this batch commits. At the cap (`max_blocks_per_commit`, default
  64) and again at the request's end, the batch closes: fdatasync the
  staged files concurrently, take every stripe in ONE sorted acquisition,
  rename, fsync each directory once, then ONE transaction carrying every
  insert and rc bump, then ONE persist, then release and ack. Stripes are
  sorted by stripe INDEX, not by hash -- hash order is not a total order
  over stripes, and using it deadlocks.
- **Group commit (ADR 0011, off by default).** With `store.group_commit`
  on, the closing step goes through a station
  (`cas-storage/src/cas/group_commit.rs`): batches queue while one group
  commits, and the committer takes everything queued as the next group --
  so an uncontended request commits immediately and grouping adds nothing
  to a lone ack. The same cap bounds the group. A group transaction that
  errors rolls back and each member replays as its own transaction under
  the stripes the group already holds, so one bad member fails one
  request.
- **What a crash leaves.** A kill between a batch's directory sync and its
  commit leaves up to `max_blocks_per_commit` block files with no record:
  residue class 1 (ADR 0005), which fsck collects and which a later PUT of
  the same content adopts in place. The batch changed the residue's SIZE,
  not its class, because the file-first ordering is preserved wholesale.
- **DELETE** (`cas-storage/src/cas/delete_path.rs`): the object record
  is read and removed in one namespace-DB transaction (idempotent,
  defeats double-DELETE double-decrements), then each block occurrence
  is decremented under its stripe in its own blocking closure; at rc==0
  the record removal commits first and the unlink follows inside the
  same stripe hold. Per-block failures are logged and the loop continues
  -- a failed decrement strands one block, aborting would strand every
  remaining one.
- **Cancellation.** Guards move into the blocking closures, which run to
  completion even when the awaiting request future is dropped; residue
  is a completed bump or a record+file without an object -- leak class,
  never a torn state. A cancel during staging leaves only temp residue,
  which the next store open purges.
- **Observability.** `s3_data_block_disk_ops_inflight` gauges submitted
  vs completed blocking closures (blocking-pool queue depth); the
  `BlockWriteGuard` pending/written/error/dropped counters survive from
  the previous design.

## Content-addressed namespaces (ADR 0014)

respcas addresses blocks since ADR 0014. A namespace carries a `key_mode`
in its metadata (`respcas/src/storage.rs`, msgpack in the `_BUCKETS` value),
and `KeyMode::Cas` changes what a key means there:

- **The key IS the value's address**: BLAKE3-256, 32 raw bytes on the wire
  (`respcas/src/content.rs:53`, `CAS_KEY_LEN`). Fixed at 32 whatever the
  store's block width is, so a client can compute keys with stock `b3sum`
  and no knowledge of the chunking.
- **Two ways in, one path through.** `SET "" <value>` and `CSET <value>`
  are the server-hashed form (the reply is the computed key);
  `SET <32-byte key> <value>` is the client-hashed form. A key of any other
  length in a Cas namespace is an error.
- **Presence is checked before anything is verified or written.** Present
  in this namespace: ack, discard the bytes, change nothing. Present in
  another Cas namespace: clone the record by reference
  (`cas-storage/src/cas/clone_path.rs`), discard the bytes. Nowhere: the
  client-hashed form verifies `blake3(value) == key`, then stores. The
  invariant is that bytes are verified against their address exactly once,
  on the write that first materializes them -- so `GET H` can only ever
  return bytes that hash to `H`.
- **Only Cas namespaces are clone sources.** A 32-byte key in a user-keyed
  namespace is a coincidence, never a verified address.
- **Storage is the ordinary engine.** At or below the inline threshold the
  record stays `Inline`; above it the value goes through the ADR 0006 write
  path and the record is `SinglePart { blocks }`. A namespace is a bucket
  to the metastore, so no new write protocol was added.
- **Store layout.** A new respcas store is `{store_header.bin, db/,
  blocks/}` -- the same meta+blocks pair every other store here is. A store
  created before ADR 0014 has its fjall files directly in the data
  directory and is opened there unchanged (fjall's own `version` marker is
  what tells the two shapes apart, `respcas/src/storage.rs:120`); it gains
  `blocks/` additively. Nothing is moved and nothing is migrated.
- **Compatibility gate.** The first `NSSET <ns> key_mode cas` raises the
  QSST header to v4 BEFORE writing metadata an older build's msgpack
  decoder cannot read, so such a build refuses the store at the open rather
  than failing mid-serve.
- **Ingest is bounded, not streamed.** `resp.max_value_size` (default 64
  MiB, `config::DEFAULT_RESP_MAX_VALUE_SIZE`) is checked against the
  DECLARED bulk length before the value is buffered, so an oversized value
  is refused while the read buffer still holds only headers.

`clone_object_by_reference` is worth one more line because ADR 0014 asked
for something the store cannot do. The ADR specified record-insert plus rc
bumps in one transaction; the record lives in the namespace database and
`_BLOCKS` in the blocks database, and no fjall transaction spans two
databases. What was built instead is the ordering plus the stripes:
references are acquired first, one per block under that block's stripe, then
the record is written; any refusal releases what was already taken. The
clone-versus-DELETE race resolves to the two outcomes the ADR wanted -- the
bump lands first and the block survives, or the DELETE's last decrement
lands first and the clone returns `None` so the caller falls through to the
ordinary verified write. A crash in the middle leaves an over-count (fsck
INFO, collected by the next recount), never a record naming a dead block.

Two command semantics follow from the model:

- **`CHECK <key>`** re-hashes the value and compares it against the key in a
  Cas namespace -- streaming the blocks if the record is block-backed -- which
  is strictly stronger than the MD5 comparison it performs in a user-keyed
  namespace. The stored MD5 stays as the record envelope's field and carries
  no authority in Cas mode.
- **`EXISTS <key>`** is the probe a dedup-upload client uses to skip a
  transfer, and it is namespace-scoped and advisory: another client may DEL
  the key between the probe and the client's decision. The hard guarantee is
  `cas + worm`, where DEL is refused and an EXISTS answer is permanent.
