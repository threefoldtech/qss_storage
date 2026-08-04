# As-Built Documentation

A description of the qss_storage codebase as it actually is, not as designed.
Where the code and the ADRs disagree, these documents follow the code and say
so explicitly.

## Scope and vantage point

Current as of branch `development` at commit `62fdf27` (2026-08-04), which is
ADR 0014 as built.

`main` is a different codebase. It is still at `c12f930`: a separate
`metastore` crate, `respd/` rather than `respcas/`, `s3cas/src/s3fs.rs`
rather than `api.rs`, and two ADRs rather than fourteen. Nothing in these
documents describes `main`. See
[04-code-health.md](./04-code-health.md#p1-adrs-describe-a-layout-main-does-not-have).

## Documents

| Document | Contents |
|----------|----------|
| [01-architecture.md](./01-architecture.md) | Crate topology, provenance, process model, request paths, backends, CI |
| [02-storage-model.md](./02-storage-model.md) | On-disk format, block addressing, refcounting, keyspaces, durability, the block protocol, content-addressed namespaces |
| [03-crates.md](./03-crates.md) | Per-crate detail: cas-storage, s3cas, respcas, benches, qss-storage-fsck |
| [04-code-health.md](./04-code-health.md) | The 2026-07-30 review, each finding annotated with its status at HEAD |

## Executive summary

qss_storage is a five-member Rust workspace: one shared content-addressed
storage library (`cas-storage`, ~27.8k lines) and four consumers of it --
`s3cas` (S3 API, ~4.9k), `respcas` (Redis/RESP2 subset, ~6.2k),
`qss-storage-fsck` (offline scrub and repair, ~1k) and `qss-benches`
(criterion). Objects are chunked into 1 MiB blocks, addressed by BLAKE3
(ADR 0002; the S3 ETag stays MD5), deduplicated and reference counted.
Metadata lives in fjall 3.x behind a `Store` trait with one implementation,
the transactional `FjallStore` (the non-transactional `FjallStoreNotx` was
removed by ADR 0007).

Four things a reader should know before touching the code:

1. **`cas-storage`'s fork lineage is history, not a constraint.** It was
   vendored from `github.com/threefoldtech/s3-cas @ b28eac0` (2026-05), and
   the ownership decision of 2026-07-30 made qss_storage its primary home:
   nothing is upstreamed, there is no rebase to protect, and the directory
   may be refactored freely. The nine `tfstor-extension` marker regions that
   survive are a change record. `cas-storage/EXTENSIONS.md` is the
   provenance document.

2. **The on-disk format is versioned and refuses rather than migrates.**
   Every metadata database carries a 32-byte QSST header. A store is created
   at version 3 and this build opens `{3, 4}`; version 4 is raised on a live
   store when a respcas content-addressed namespace is first created (ADR
   0014). Older versions are refused at open with an operator-readable
   message naming the store. Details in
   [02-storage-model.md](./02-storage-model.md#store-header-qsst).

3. **Both frontends address blocks.** Since ADR 0014 respcas is not a
   metadata-only key-value store: a `KeyMode::Cas` namespace keys records by
   the BLAKE3-256 of the value, routes anything above the inline threshold
   through the same ADR 0006 write path s3cas uses, and clones existing
   content by reference rather than storing it twice. A respcas store is the
   same meta+blocks pair every other store here is, which is what lets fsck
   walk it.

4. **The durability boundary is the acknowledgement, not the block.** ADR
   0010 made one request one durability unit; ADR 0011 optionally merges the
   closing step of concurrent requests; ADR 0013 gave each tree an ack class,
   so a write a client was told succeeded is persisted before the reply
   unless the protocol has a loud, retryable answer for losing it.

## Health summary

[04-code-health.md](./04-code-health.md) is a dated review (2026-07-30,
branch `refactor/cas-storage` at `e349d9d`), kept as a record and annotated
finding by finding with its status at `62fdf27`.

| Series | Count | Status at `62fdf27` |
|--------|-------|---------------------|
| H1-H6, correctness / soundness | 6 | all RESOLVED |
| H7-H12, design / consistency | 6 | 4 RESOLVED, H9 CLOSED (correct as written), H10 PARTIAL (sites fixed, class recurred), H12 OBSOLETE |
| B1-B4, bloat / duplication | 4 | B1 OBSOLETE, B3 RESOLVED, B2 and B4 PARTIAL |
| P1-P6, hygiene / process | 6 | 3 RESOLVED, P5 OBSOLETE, P1 and P2 OPEN on `main` |

Nothing in the correctness series is open. What remains is P1/P2 -- `main`
carries a codebase the ADRs no longer describe -- and two classes that came
back rather than staying fixed: truncating casts (nothing gates the lint) and
function length (two long functions that were never the shape the finding was
about).

Verification at `62fdf27`:

- `cargo fmt --all --check`: clean
- `cargo clippy --workspace --all-targets -- -D warnings`: clean
- `cargo test --workspace`: 455 passed, 0 failed, 1 ignored
- `cargo clippy -- -W clippy::pedantic`: 758 warnings (not gating)
