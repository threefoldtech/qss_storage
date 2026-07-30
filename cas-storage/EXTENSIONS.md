# cas-storage provenance and change record

> **Ownership decision (2026-07-30):** qss_storage is now the primary home
> of this code. The upstream (`threefoldtech/s3-cas`) and the older
> lineages it descends from are considered obsolete; there is no plan to
> rebase on upstream or to upstream changes back. This directory is owned
> code and may be refactored freely. The `tfstor-extension` markers and
> the sections below are retained as a historical record of how this tree
> diverged from the vendored snapshot, not as a rebase manual. A broader
> renaming pass (tfstor -> qss) is planned separately.

> Naming note: `tfstor` is the former name of this repository (renamed to
> `qss_storage` in 2026-07).

This `cas-storage/` was originally vendored from
`https://github.com/threefoldtech/s3-cas` at commit **`b28eac0`** (2026-05),
then extended for the `respd` Redis-protocol frontend and subsequently
fixed and refactored in place (see "What was fixed" below).

## What was added

### `metastore::traits::BaseMetaTree::len` / `is_empty`
Upstream marks both as `#[cfg(test)]`. We need them at runtime for respd
commands (`LENGTH`, `DBSIZE`). Promoted out of `cfg(test)`; default impl
of `is_empty` retained.

### `metastore::traits::MetaTreeExt::iter_kv(start_after)`
Forward iteration from an arbitrary key (used by respd `SCAN` cursor).
Upstream only has `iter_all()` (== `iter_kv(None)`).

### `metastore::traits::MetaTreeExt::iter_kv_backward(start_key)`
Backward iteration from an arbitrary key (used by respd `RSCAN`).
No upstream equivalent.

## What was fixed

These are bug fixes against upstream rather than new API, but they change
upstream code, so they carry the same markers.

### `FjallStore::num_keys` (transactional backend)
Upstream is `unimplemented!("fjall with transaction does not support number
of keys")`, which panics on the default `--metadata-db fjall` path of
`s3cas inspect num-keys`. It does support it: `FjallTree::len` in the same
file counts via `db.read_tx().len(&partition)`. `num_keys` now opens the
named partition and does the same, mirroring `FjallStoreNotx::num_keys`.
Note the two backends differ in exactness: the transactional one is exact,
`fjall_notx` uses `approximate_len`, as upstream does.

### `MetaStore::num_keys` returns `Result`
Upstream returns a bare `usize` and `unwrap()`s the store call, so a backend
error becomes a panic in a method documented as a monitoring/debugging
helper. Now `Result<usize, MetaError>`. No in-tree caller (`s3cas inspect`
goes through `Store::num_keys` directly), so this is a free signature change
here; it is a breaking change for upstream's public API.

### Checked UTF-8 on values read back from disk
Six sites used `String::from_utf8_unchecked` with a comment arguing that only
valid strings are ever written. That covers the write path; these bytes come
back out of a database file, where corruption, a truncated write or a format
change breaks the invariant, and the penalty is undefined behaviour rather
than an error. All six now validate:

- `metastore/bucket_meta.rs` (bucket name) -> `FsError::InvalidUtf8`
- `cas/multipart.rs` (bucket, key, upload_id) -> `FsError::InvalidUtf8`
- `metastore/stores/fjall_common.rs` (`range_filter` key; was one copy per
  backend before the dedup below)

The first two now go through `Reader::utf8` (`metastore/codec.rs`), added by
the format v1 work below, rather than a hand-written `String::from_utf8` per
field. The behaviour is the same error; the marked regions in
`bucket_meta.rs` and `multipart.rs` moved onto the `Reader` calls, and
`multipart.rs`'s two separate regions became one covering all three string
fields.

The first two sit in `TryFrom` impls and simply return the error. The
`range_filter` site does not: the trait method yields an infallible
`(String, Object)` item, so changing the signature would ripple into
`s3cas::s3fs`. There, a key that is not valid UTF-8 is logged and skipped,
which is how the same iterator already handles keys the backend fails to read
(`filter_map(|g| g.into_inner().ok())` a few lines above). The value decode in
the same closure is skipped and logged for the same reason. Anyone changing
`range_filter` to be fallible should revisit all three.

### `SAFETY` argument for the `'static` transaction transmute
`FjallStore::begin_transaction` launders a `SingleWriterWriteTx<'_>` to
`'static`. Upstream's comment asserts the conclusion ("won't outlive the
store") without the two facts it rests on, both silently breakable: the
`Arc<FjallStore>` held by `FjallTransaction` keeps the database alive, and
the field declaration order (`tx` before `store`) makes the transaction drop
first. Both are now written down, at the transmute and on the struct, with
the field order flagged as load-bearing.

### `unsafe impl Sync for FjallTransaction` deleted
Never needed. `MutexGuard<'_, T>` is `Sync` when `T: Sync`, so the struct gets
an ordinary auto `Sync` impl and the `TransactionBackend: Send + Sync`
supertrait bound is satisfied without an assertion. Replaced by a `const _`
static assertion, so a future non-`Sync` field fails at the definition rather
than being papered over.

`unsafe impl Send` is required and stays: the build fails without it, because
`SingleWriterWriteTx` holds a `std::sync::MutexGuard`, which is `!Send`. It now
carries a written argument, including the part that is an assumption rather
than a proof -- a mutex guard released on a thread other than the one that
took it is only sound on some platforms, and what makes it a non-issue here is
that the single caller never holds a transaction across an `.await`.

### `unsafe impl Send`/`Sync for FjallNoTransaction` deleted
Both redundant: the fields (`Arc<FjallStoreNotx>`, `Vec<(String, Vec<u8>)>`)
are `Send + Sync`, so the auto impls apply. Same `const _` static assertions
in their place.

### `unsafe impl Sync for BlockStream` deleted
Also never needed, also unjustified. Every field is already `Sync`, including
`open_fut`, whose boxed future carries an explicit `+ Send + Sync` bound in the
struct definition. Same `const _` static assertion in its place.

### Backend-agnostic `num_keys` test
`stores/test_utils.rs` gained `test_num_keys` plus a `num_keys` method on the
`TestStore` trait, wired into both backends' test modules. Upstream had no
coverage for `Store::num_keys`, which is how the `unimplemented!()` survived.

### `Durability` mapping untangled (H12)
The enum's doc comments always described POSIX semantics (`Fsync` = data +
metadata, strongest; `Fdatasync` = data only, weaker), but the mapping onto
fjall was crossed: `Fsync` -> `SyncData`, `Fdatasync` -> `SyncAll`, with
`Fdatasync` as the default. Swapped so the names tell the truth
(`Fsync` -> `SyncAll`, `Fdatasync` -> `SyncData`) and the defaults (library,
respd, s3cas CLI) moved from `Fdatasync` to `Fsync` -- so the default persist
behavior is bit-for-bit unchanged, and only explicit flag users see a change:
they now get what the flag name promised.

### The two fjall backends deduplicated
Finding B1 of `docs/as-built/04-code-health.md`. `stores/fjall.rs` and
`stores/fjall_notx.rs` were near-copies: same function inventory, 161
identical non-trivial lines, and every extension (`iter_kv`, the checked
UTF-8 keys, the `num_keys` fix) had to be written twice.

The shared half now lives in `stores/fjall_common.rs`, generic over a
`FjallFlavor` trait: `FjallStoreOf<F>` (partition cache, tree opening,
`Store` impl, inlined-metadata threshold, disk space) and `FjallTreeOf<F>`
(the whole `BaseMetaTree` and `MetaTreeExt` impl, including `range_filter`
and the two `iter_kv` walks). `FjallStore` and `FjallStoreNotx` are now
aliases of that generic with their flavor marker; their public API,
constructors and `Debug` output are unchanged.

What did *not* unify, and why:

- **Writes.** `SingleWriterTxKeyspace::insert`/`remove` wrap the call in a
  fjall write transaction; `Keyspace::insert`/`remove` do not. Same names,
  different machinery, no common trait -- they stay per-flavor one-liners.
- **Reads.** The transactional keyspace handle has no iteration API at all,
  so `range`/`prefix`/`len` there go through `db.read_tx()`, while the plain
  keyspace iterates directly (and at `SeqNo::MAX` rather than a snapshot
  seqno). The flavor supplies the iterator; both return a plain
  `fjall::Iter`, which owns its snapshot nonce and so survives the read
  transaction that produced it.
- **The transaction backends.** A real fjall write transaction versus a
  no-op transaction that replays its own inserts on rollback. This is the
  reason both backends exist; it was never duplication.
- **`Store::num_keys`.** Still exact on the transactional backend and
  `approximate_len` on the other, as recorded above -- now an explicit
  per-flavor method with that difference documented on the trait.

Two behaviour notes: the plain backend inherits the partition cache that
only the transactional one had (fewer keyspace-lock acquisitions, and
`tree_delete` now evicts on both), and `get_partition` propagates a
keyspace-open failure as `MetaError` instead of the transactional path's
former `.expect("Can open keyspace")`.

The shared test battery moved into a `backend_test_battery!` macro in
`stores/test_utils.rs`, so both backends still run the identical three
tests from one definition instead of two copies.

Because the dedup dissolved the code they fenced, the `tfstor-extension`
markers in these two files are gone; this entry and the comments in
`fjall_common.rs` are the record.

## What diverged wholesale (ADR 0002, 2026-07-30)

The BLAKE3 migration (`docs/adr/0002-blake3-hash-migration.md`, commits
`8ed2506`..`69c726e`) rewrote parts of this tree that upstream owns, rather
than extending them. No marker regions were added for it: the changes are the
files, not fenced blocks inside them, and per the ownership decision at the
top of this document there is no rebase to protect. Recorded here so the
divergence from the `b28eac0` snapshot stays legible:

- **On-disk record format v1** (`a0c6471`). Every length and count field is
  `u64` LE instead of native-width `usize`; `metastore/constants.rs` and its
  `PTR_SIZE` are deleted. A new `metastore/codec.rs` (`Reader`, id-list
  helpers) gives all four record types checked offset arithmetic, exact-length
  enforcement and typed errors. Upstream's `MultiPart` decoder absorbed
  trailing garbage as extra block ids through a `chunks_exact` loop over the
  remainder; the id list is now framed by an `id_width` byte plus a count, so
  a record decodes without knowing which store wrote it and trailing bytes are
  reported. Golden byte-layout fixtures pin the layout in each record's test
  module. Format-breaking, with no migration.
- **Typed decode errors** (`8ed2506`). `FsError`'s blanket `MalformedObject`
  is replaced by `Truncated`, `TrailingBytes`, `InvalidUtf8`,
  `UnknownObjectType`, `InvalidIdWidth` and `LengthOverflow`, plus
  `MetaError::Corruption`. Seven deserialize sites that panicked now
  propagate.
- **`ContentHash` split from the block address** (`7fcc191`). Upstream types
  `Object.hash` and `MultiPart.hash` as `BlockID` while storing MD5 ETag
  digests in them. They are now `ContentHash`, a fixed 16-byte newtype, so
  making the block address a per-store width could not silently widen S3
  ETags. `s3cas`'s multipart ETag was also fixed to the S3 convention (MD5 of
  the concatenated part MD5s) from hashing raw block addresses.
- **`BlockId` newtype** (`c6991e3`). `[u8; 16]` becomes `{ bytes: [u8; 32],
  len }` with zeroed padding, so the derived `Eq`/`Hash`/`Ord` agree with
  `as_slice()`. Upstream's prefix-path search could exhaust its prefixes and
  write an empty path that then panicked in `Block::disk_path`; it now covers
  the full width and errors on true exhaustion.
- **`hasher.rs`, `metastore/store_header.rs`, `config.rs`** -- net-new modules
  with no upstream counterpart: the BLAKE3 `Hasher` enum, the 32-byte QSST
  store header written by `MetaStore::open_or_create` (with the underscore
  bucket-name guard that protects the reserved trees), and the
  `qss_storage.toml` loader.

MD5 remains a dependency of this crate for exactly one reason: the ETag digest
is computed here, in `cas/write_path.rs`, not in the frontends.

## Implementations

The trait additions are implemented in:

- `src/metastore/stores/fjall_common.rs` (shared, generic over the flavor)
- `src/metastore/stores/fjall.rs`        (transactional flavor)
- `src/metastore/stores/fjall_notx.rs`   (non-transactional flavor)

Both backends preserve upstream's partition cache, durability handling, and
write-path locking semantics. They share their range plumbing with the
existing `iter_all` (which is now a one-line wrapper around `iter_kv(None)`).

## Upstreaming sketch (historical -- moot per the 2026-07-30 ownership decision)

Kept for the record only; no upstream PRs are planned. A PR to
threefoldtech/s3-cas would have:

1. Add `fn iter_kv(Option<Vec<u8>>)` and `fn iter_kv_backward(Option<Vec<u8>>)`
   to `MetaTreeExt`, document them, and adjust `iter_all` to be a default
   method that calls `iter_kv(None)` (so existing callers stay green).
2. Drop the `#[cfg(test)]` on `BaseMetaTree::len` and `is_empty`.
3. Port both `fjall.rs` and `fjall_notx.rs` impls (copy from this fork).
4. Reference respd's RSCAN/SCAN/DBSIZE use cases as motivation.

The `num_keys` fixes are worth a separate, smaller PR that stands on its own
(no respd context needed): implement `FjallStore::num_keys`, make
`MetaStore::num_keys` fallible, add `test_num_keys` to the shared backend
test battery.

The `unsafe` cleanup (checked UTF-8, the transmute `SAFETY` argument, and the
two deleted `unsafe impl Sync`s) is a third PR candidate, and the most
obviously upstreamable of the three: it removes undefined behaviour on
corrupt input, deletes two `unsafe` impls the compiler proves unnecessary, and
adds no API surface. Nothing in it depends on respd or on anything else in
this fork. The only judgement call a reviewer might push back on is the
skip-and-log behaviour for non-UTF-8 keys in `range_filter`; the alternative
is making that trait method fallible, which is a breaking change and belongs
in its own discussion.
