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
