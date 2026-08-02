# Initial Architecture Overview

**Status**: Accepted
**Date**: 2026-05-26
**Updated**: 2026-05-26 (resolved fjall_notx and migration questions; clarified MD5/BLAKE3 path and SharedBlockStore cross-protocol semantics)

> Naming note (2026-08-02): the RESP daemon this and every earlier ADR calls
> `respd` is now the `respcas` crate and binary. The old name is left as
> written wherever it appears in an accepted decision, the way `tfstor` is.

---

## Context

TFStor is a Rust workspace that provides storage solutions built on
content-addressed storage (CAS) with block-level deduplication. The codebase
is currently shipping two user-facing servers (`s3cas` and `respd`) backed by
a shared library (`cas-storage`). This first ADR records the architecture as
it stands today so future decisions have a baseline to reference, supersede,
or amend.

A recent refactor consolidated what used to be a separate `metastore` crate
and the `s3cas/src/cas/` tree into a single `cas-storage` crate. This ADR
captures that current shape, not the pre-refactor layout.

Related design notes:
- `docs/refcount.md` - failure-mode contract for block reference counting.

---

## Decision

Adopt a three-crate workspace with a single shared CAS library and two
independent protocol frontends:

- `cas-storage` - the storage engine (library only).
- `s3cas` - S3-compatible HTTP server.
- `respd` - Redis-compatible TCP server.

All deduplication, refcounting, metadata, and on-disk layout decisions live in
`cas-storage`. Frontends translate protocol semantics into `cas-storage` API
calls and own only protocol-level concerns (auth, request parsing, response
shaping, metrics).

---

## Architecture Overview

### Component Breakdown

1. **cas-storage** (`cas-storage/`)
   - Library crate. No `main`.
   - `cas/` - block-oriented data path: streaming reads and writes,
     multipart uploads, range requests, byte streams, shared block store,
     per-namespace `CasFS` facade (`single_namespace` and multi-namespace
     constructors).
   - `metastore/` - pluggable metadata layer. `traits.rs` defines the store
     interface; `stores/fjall.rs` (transactional) and `stores/fjall_notx.rs`
     (non-transactional) are the current backends. `block.rs`,
     `bucket_meta.rs`, and `object.rs` model the persisted records.
   - Content addressing: objects are chunked into 1 MiB blocks, identified by
     MD5. Small objects can be inlined in metadata.
   - Deduplication: refcounted blocks, governed by the contract in
     `docs/refcount.md` ("data leakage allowed, data loss never").

2. **s3cas** (`s3cas/`)
   - Binary. S3 protocol via the `s3s` crate served on `hyper`.
   - `s3fs.rs` adapts `cas-storage::CasFS` to the `s3s` filesystem trait.
   - Operational utilities live alongside (`check.rs`, `inspect.rs`,
     `retrieve.rs`, `metrics.rs`).

3. **respd** (`respd/`)
   - Binary. RESP (Redis) protocol on raw TCP.
   - `namespace.rs` provides Redis-style namespace isolation on top of the
     shared CAS store; `cmd.rs` dispatches commands (including
     non-standard commands like `RSCAN`); `storage.rs` is the CAS bridge.

### Data Flow

```
   S3 client                    Redis/RESP client
       |                              |
   [ s3cas ]                       [ respd ]
       |                              |
       \------> cas-storage::CasFS <--/
                       |
        +--------------+--------------+
        |                             |
   block I/O                    metastore (fjall / fjall_notx)
   (1 MiB chunks,                (objects, blocks, buckets,
    MD5-addressed,                refcounts)
    refcounted)
        |
   on-disk block files
```

### Key invariants

- Block writes must increment refcount before, or atomically with, the block
  becoming visible. Failing to increment refcount is treated as a fatal
  error; failing to decrement is acceptable (causes leakage, not loss).
- A `SharedBlockStore` may be shared across multiple `CasFS` namespaces; each
  namespace owns its own metadata but participates in global block dedup.
- Both metastore backends (`fjall`, `fjall_notx`) must satisfy the same
  `MetaStore` trait; switching backends is a configuration choice, not an
  API change.

---

## Alternatives Considered

| Alternative                                         | Pros                                          | Cons                                                    | Why Not                                              |
|-----------------------------------------------------|-----------------------------------------------|---------------------------------------------------------|------------------------------------------------------|
| Keep `metastore` as a separate crate                | Strong physical boundary                      | Two crates always change together; added build cost      | Boundary was paper-thin; module boundary inside `cas-storage` is enough |
| Embed CAS logic per frontend (no shared library)    | Per-frontend tuning                           | Duplicated dedup and refcount logic, drift risk         | Refcount correctness is too important to fork        |
| Pick one metastore backend                          | Less code                                     | No room for non-transactional benchmarking / fallback   | We actively benchmark both (`benches/fjall_benchmark.rs`) |
| Use a different chunking strategy (CDC / variable)  | Better dedup ratios for some workloads        | More complex, harder to reason about, slower hot path   | Fixed 1 MiB blocks are simple and sufficient today   |

---

## Consequences

### Positive
- One place owns dedup, refcounting, and on-disk format.
- Frontends stay thin and protocol-focused.
- Backends are swappable behind a trait, which keeps benchmarking honest.
- Multi-namespace deployments share blocks globally without leaking metadata
  between tenants.

### Negative
- Any breaking change in `cas-storage` ripples through both frontends
  simultaneously.
- The `MetaStore` trait is now a hot contract; extending it requires touching
  every backend.

### Risks
- Refcount correctness under concurrent writes / crashes is the single most
  important property; regressions here can cause silent data loss.
  Mitigation: keep the refcount contract in `docs/refcount.md` authoritative
  and add tests for every change to block lifecycle.
- MD5 as the content address. Acceptable for dedup but not a security
  property. Mitigation: never expose the hash as an integrity guarantee to
  external clients.

---

## Implementation Plan

This ADR documents the current state, so the implementation already exists.
Follow-up ADRs are expected for:

1. A formal description of the on-disk layout and migration policy.
2. A decision on whether to keep both Fjall backends long-term or pick one.
3. Authentication and multi-tenant policy for `s3cas` (currently single-key).
4. RESP command surface scope and compatibility goals for `respd`.

---

## Resolved During Review

- **fjall_notx stays.** Non-transactional fjall is kept as a first-class
  backend, not a benchmarking artifact. Its role is low-latency / high-
  throughput workloads where the data is non-critical (acceptable loss on
  crash). Per-deployment choice via `--metadata-db`.
- **On-disk migration is a non-issue for blobs.** Blocks are
  content-addressed and immutable, so the blob format is effectively frozen
  by construction. A metadata format migration is conceivable but parked as
  a far-future concern; no design work is needed now.
- **Hash algorithm is a deployment-time choice.** Whatever the chosen hash,
  it is baked into every content address in a store and cannot be changed
  without re-addressing every block. Mixing MD5 and a different hash inside
  one `SharedBlockStore` is therefore forbidden.

## Open Questions

- [ ] **Move to BLAKE3** (faster and cryptographically stronger than MD5).
  Subtasks:
  - Decide whether the hash becomes a build-time const or a per-store
    runtime parameter persisted in the store header. Runtime is preferred
    so existing MD5 stores keep working.
  - Plumb the choice through `SharedBlockStore::new` and the public
    constructors (`CasFS::single_namespace`, multi-namespace).
- [ ] **Introduce a config file.** Currently both binaries are clap-only
  (`s3cas/src/main.rs:25`, `respd/src/main.rs:16`). The MD5 -> BLAKE3 work
  is the natural forcing function: hash choice plus durability, inline
  threshold, ports, and credentials are getting numerous enough that a
  TOML config (with CLI overrides) starts to pay off. Open: do this before
  or together with the BLAKE3 change.
- [ ] **Cross-protocol block sharing via `SharedBlockStore`.** The library
  already supports a single `Arc<SharedBlockStore>` feeding multiple
  `CasFS` namespaces, so blocks would dedupe across `s3cas` and `respd`
  if both processes shared the store. Today neither binary does this and
  fjall is not multi-process-safe. Open: is the right shape a combined
  binary, an in-process router, or do we leave protocols isolated?
