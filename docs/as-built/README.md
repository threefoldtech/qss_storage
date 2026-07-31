# As-Built Documentation

A description of the qss_storage codebase as it actually is, not as designed.
Where the code and the ADRs disagree, these documents follow the code and say
so explicitly.

## Scope and vantage point

Written against branch `refactor/cas-storage` at commit `e349d9d`, which is
`main` (`c12f930`) plus the `metastore` -> `cas-storage` consolidation.

This matters: **`main` and this branch have materially different crate
layouts.** `main` still ships a separate `metastore` crate and an
`s3cas/src/cas/` tree. ADRs 0001 and 0002 on `main` describe the
`cas-storage` layout that only exists here. See
[04-code-health.md](./04-code-health.md#p1-adrs-describe-a-layout-main-does-not-have).

## Documents

| Document | Contents |
|----------|----------|
| [01-architecture.md](./01-architecture.md) | Crate topology, process model, request paths, vendored-fork status |
| [02-storage-model.md](./02-storage-model.md) | On-disk format, block addressing, refcounting, keyspaces, durability |
| [03-crates.md](./03-crates.md) | Per-crate detail: cas-storage, s3cas, respd, benches |
| [04-code-health.md](./04-code-health.md) | Bloat and smell findings, prioritized, with locations |

## Executive summary

qss_storage is a three-crate Rust workspace: one shared content-addressed
storage library (`cas-storage`, 4884 lines) and two independent protocol
frontends over it -- `s3cas` (S3 API, 2368 lines) and `respd` (Redis/RESP
subset, 4058 lines). Objects are chunked into 1 MiB blocks, addressed by BLAKE3
(ADR 0002; the S3 ETag stays MD5), deduplicated, and reference counted.
Metadata lives in fjall 3.x, behind a `Store` trait with one
implementation: the transactional `FjallStore` (the non-transactional
`FjallStoreNotx` was removed by ADR 0007).

Three things a reader should know before touching the code:

1. **`cas-storage` is a vendored fork**, not original code. It was copied from
   `github.com/threefoldtech/s3-cas` at commit `b28eac0` (2026-05). Four
   blocks are marked `tfstor-extension: BEGIN/END`; everything else is meant
   to stay byte-identical to upstream so the snapshot can be re-based. Edits
   outside those markers make rebasing harder and should be deliberate.

2. **The on-disk format is v1 and has no backward compatibility.** All length
   and count fields are fixed `u64` (the old `PTR_SIZE = size_of::<usize>()`
   is gone), records with block-id lists carry a self-describing width byte,
   and every record is length-exact. A store written before format v1 reads as
   a decode error. Details in
   [02-storage-model.md](./02-storage-model.md#on-disk-record-formats-v1).

3. **The health review found 6 correctness or soundness issues**, one of them
   a confirmed reachable panic on default flags, and one a cross-tenant data
   substitution risk arising from MD5 plus shared block storage. See
   [04-code-health.md](./04-code-health.md).

The codebase is in reasonable shape structurally -- the refactor genuinely
improved it, the trait boundaries are clean, and test coverage of the refcount
contract is real. The problems are concentrated in the unsafe code and the
serialization layer, both inherited from upstream.

## Health summary

| Series | Count | Theme |
|--------|-------|-------|
| H1-H6, correctness / soundness | 6 | Reachable panic, unaudited `unsafe`, format portability, MD5 collision exposure |
| H7-H12, design / consistency | 6 | Unverified Content-MD5, truncating casts, edition and naming drift |
| B1-B4, bloat / duplication | 4 | Duplicated store backends, oversized functions, lint backlog |
| P1-P6, hygiene / process | 6 | Red CI on main, stale docs, missing fmt gate |

The single most actionable item is **H1**: a confirmed panic on a default code
path whose own error message is wrong about why it cannot be implemented, with a
few-line fix available in the same file. Suggested order of work is at the end
of [04-code-health.md](./04-code-health.md#suggested-order-of-work).

**Update 2026-07-30:** a remediation pass on the `development` branch fixed
H1, H2, H5, H6, H8, B2, B3, P3, P4, and P6, recorded H3/H4 in ADR 0002, and
additionally revived the dead benchmark suite and fixed a port race in the
respd test harness. Live status table at the top of
[04-code-health.md](./04-code-health.md#resolution-status-2026-07-30-branch-development).

Verification state at time of writing, on this branch:

- `cargo clippy --workspace --all-targets -- -D warnings`: clean
- `cargo test --workspace`: 44 passed, 0 failed
- `cargo clippy -- -W clippy::pedantic`: 458 warnings (not gating)
