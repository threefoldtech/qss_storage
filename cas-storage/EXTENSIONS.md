# cas-storage extensions (tfstor fork)

This `cas-storage/` was vendored from
`https://github.com/threefoldtech/s3-cas` at commit **`b28eac0`** (2026-05).

The tfstor workspace ships a small extension to the upstream trait surface,
needed by the `respd` Redis-protocol frontend that lives alongside `s3cas`
in this repo. Everything else is byte-identical to the upstream snapshot.

If you're rebasing on a newer upstream:

1. Replace this directory with the new upstream snapshot.
2. Re-apply the additions listed below (each block is marked with
   `// ---- tfstor-extension: BEGIN/END ----`).
3. Run `cargo test -p cas-storage -p respd -p s3cas`.

The end goal is to upstream these so the fork can dissolve.

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

## Upstreaming sketch

A PR to threefoldtech/s3-cas would:

1. Add `fn iter_kv(Option<Vec<u8>>)` and `fn iter_kv_backward(Option<Vec<u8>>)`
   to `MetaTreeExt`, document them, and adjust `iter_all` to be a default
   method that calls `iter_kv(None)` (so existing callers stay green).
2. Drop the `#[cfg(test)]` on `BaseMetaTree::len` and `is_empty`.
3. Port both `fjall.rs` and `fjall_notx.rs` impls (copy from this fork).
4. Reference respd's RSCAN/SCAN/DBSIZE use cases as motivation.

The two fixes above are worth a separate, smaller PR that stands on its own
(no respd context needed): implement `FjallStore::num_keys`, make
`MetaStore::num_keys` fallible, add `test_num_keys` to the shared backend
test battery.
