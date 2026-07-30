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

- `metastore/bucket_meta.rs` (bucket name) -> `FsError::MalformedObject`
- `cas/multipart.rs` (bucket, key, upload_id) -> `FsError::MalformedObject`
- `metastore/stores/fjall.rs` and `stores/fjall_notx.rs` (`range_filter` key)

The first two sit in `TryFrom` impls and simply return the error. The two
`range_filter` sites do not: the trait method yields an infallible
`(String, Object)` item, so changing the signature would ripple into
`s3cas::s3fs`. There, a key that is not valid UTF-8 is logged and skipped,
which is how the same iterator already handles keys the backend fails to read
(`filter_map(|g| g.into_inner().ok())` a few lines above). Anyone changing
`range_filter` to be fallible should revisit both.

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

## Implementations

The trait additions are implemented in:

- `src/metastore/stores/fjall.rs`     (transactional backend)
- `src/metastore/stores/fjall_notx.rs` (non-transactional backend)

Both implementations preserve upstream's partition cache, durability
handling, and write-path locking semantics. They share their range
plumbing with the existing `iter_all` (which is now a one-line wrapper
around `iter_kv(None)`).

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
