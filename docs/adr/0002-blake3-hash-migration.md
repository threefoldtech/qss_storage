# BLAKE3 Hash Migration and Introduction of a Config File

**Status**: Proposed
**Date**: 2026-05-26
**Amended**: 2026-07-30 -- recorded the multi-tenant substitution risk
(making this migration a prerequisite for untrusted multi-tenancy, not an
improvement), folded fixed-width on-disk fields into the same format break,
and flagged the width default under the adversarial framing.

---

## Context

ADR 0001 noted MD5 as the content-address hash for blocks. MD5 was chosen
for simplicity and speed, but:

- It is cryptographically broken, and under shared block storage that is a
  concrete attack, not a hygiene concern. In `SharedBlockStore` mode the
  block store and `_BLOCKS` refcount tree are shared across namespaces
  (tenants), so deduplication is cross-tenant. MD5 chosen-prefix collisions
  are practical: a tenant who can upload arbitrary bytes can construct two
  distinct blocks with the same MD5, and whichever lands first wins -- the
  second writer's content is silently discarded and the first tenant's
  bytes are served in its place. **This makes the migration a prerequisite
  for offering shared-block multi-tenancy to untrusted tenants**, not a
  nice-to-have. Until it lands, either do not enable `SharedBlockStore`
  across trust boundaries, or verify full block content on every dedup hit
  instead of trusting the digest.
- BLAKE3 is faster than MD5 on modern CPUs (SIMD, parallelism) AND offers
  256-bit collision resistance.
- The hash is baked into every block address. Inside one store it must be a
  single, immutable choice (ADR 0001, Resolved).

Operational state when this ADR is written:

- Both binaries are configured only via clap CLI flags
  (`s3cas/src/main.rs:25`, `respd/src/main.rs:16`). No config file exists.
- Existing tfstor stores are throwaway development data. Backwards
  compatibility with MD5-addressed stores is **explicitly not a goal**.
- MD5 is still required by the S3 protocol for ETag computation
  (`s3cas/src/s3fs.rs:58`, `s3cas/src/check.rs:63`). This is independent
  of the storage hash and must be preserved.

This ADR bundles two coupled changes that have the same forcing function:
adding BLAKE3 as the new default block hash, and introducing a minimal
TOML config file so the new selection (and growing flag surface) has a
home.

---

## Decision

1. **Default block hash becomes BLAKE3.** MD5 is removed as a block-address
   hash. MD5 remains compiled in **only** for S3 ETag computation, which
   is a protocol requirement, not a storage choice.
2. **Hash choice and width are per-store, runtime parameters**, persisted
   in a store header written at creation time. Inside one store the hash
   is immutable for the life of the store.
3. **Hash output width is configurable per store**: either 16 bytes
   (BLAKE3 truncated to 128 bits) or 32 bytes (BLAKE3 native). The width
   is recorded in the store header alongside the algorithm.
4. **Introduce a TOML config file** as the primary configuration surface.
   CLI flags remain as overrides. The config file is the natural home for
   hash selection, durability, inline threshold, ports, and credentials.
5. **No migration from MD5-addressed stores.** Existing dev data is
   discarded. A binary that opens a store whose header does not match a
   supported BLAKE3 variant refuses to start with a clear error.
6. **All on-disk length and count fields become fixed-width `u64` little
   endian** in the same format break. Today `PTR_SIZE =
   size_of::<usize>()` is embedded in the serialization of `Block`,
   `BucketMeta`, and `MultiPart` (19 sites), so a store written on a
   64-bit host misparses on a 32-bit one and nothing records which width
   wrote it. Since this ADR already breaks the format and introduces a
   versioned store header, the pointer-width coupling is removed here
   rather than in a separate break. `PTR_SIZE` disappears from the on-disk
   layer entirely.

---

## Architecture Overview

### Hasher trait

A new `Hasher` abstraction in `cas-storage`:

```
trait Hasher {
    const ALGO_ID: u8;   // stable on-disk discriminant
    fn width(&self) -> usize;  // 16 or 32
    fn hash(&self, data: &[u8]) -> SmallVec<[u8; 32]>;
    fn streaming(&self) -> Box<dyn StreamingHasher>;
}
```

Implementations:
- `Blake3 { width: HashWidth::W16 | W32 }`
- (No `Md5` impl. MD5 stays a free function used only by the S3 ETag path.)

### Store header

Each `SharedBlockStore` writes a `_STORE_HEADER` record on creation:

```
struct StoreHeader {
    magic: [u8; 4],         // "TFST"
    version: u16,           // start at 1
    hash_algo: u8,          // Blake3 = 1
    hash_width: u8,         // 16 or 32
    created_at: u64,        // unix seconds
    reserved: [u8; 16],
}
```

On open: read the header, instantiate the matching `Hasher`, and store it
inside `SharedBlockStore`. Mismatched / unknown hashers abort cleanly.

### Threading the hasher

`Hasher` is owned by `SharedBlockStore`, then referenced by every code
path that produces a content address:

- `cas-storage/src/cas/write_path.rs` - block hash, inline hash.
- `cas-storage/src/cas/multipart.rs` - per-part hash.
- `cas-storage/src/cas/block_stream.rs` - block boundary hashing.
- `cas-storage/src/cas/read_path.rs` - verification on read.
- `s3cas/src/check.rs` - integrity check tool (block hash via store, ETag
  via fixed MD5).

### Config file

New crate-level type:

```
struct TfstorConfig {
    store: StoreConfig {
        hash: HashConfig { algo: "blake3", width: 16 | 32 },
        durability: "buffer" | "fsync" | "fdatasync",
        inline_metadata_size: Option<usize>,
        metadata_db: "fjall" | "fjall_notx",
    },
    s3: Option<S3Config { host, port, access_key, secret_key }>,
    resp: Option<RespConfig { host, port, data_dir, admin_password }>,
    metrics: MetricsConfig { host, port },
}
```

Loader order:
1. `--config <path>` if given.
2. `./tfstor.toml` if present.
3. `/etc/tfstor/tfstor.toml`.
4. Built-in defaults.

CLI flags override the equivalent config-file field. Each binary still
parses its own clap struct; the struct's defaults are populated from the
loaded config.

### Data Flow (write path, post-change)

```
client -> frontend (s3cas|respd)
       -> CasFS::put(...)
              -> SharedBlockStore.hasher.streaming()
              -> 1 MiB chunks, BLAKE3 keyed addresses
              -> metastore: insert block(addr) or bump refcount
              -> object/key metadata stored
       <- (for S3) compute MD5 ETag in parallel, return to client
```

---

## Alternatives Considered

| Alternative                                          | Pros                                            | Cons                                                              | Why Not                                                  |
|------------------------------------------------------|-------------------------------------------------|-------------------------------------------------------------------|----------------------------------------------------------|
| Keep MD5 as the block hash                           | Zero work                                       | Weak hash; not a great public signal                              | Going forward we want a defensible default               |
| Build-time hash selection (Cargo feature)            | Simplest code                                   | Can't read or write a different-hash store from the same binary   | Operators expect one binary to handle their stores       |
| Build-time, both hashes always linked, startup flag  | Simple                                          | Easy to point a binary at the wrong store and corrupt nothing yet not notice | Header-based runtime selection costs little and self-documents |
| BLAKE3 fixed at 32 bytes                             | Native output, no truncation                    | Larger keys; doubles metadata-key cost vs current MD5             | We want the width to be a deployment knob                |
| BLAKE3 fixed at 16 bytes                             | Same key width as MD5 today                     | Discards strength we just paid for                                | Some deployments will want full 256 bits                 |
| Migration / re-hash tool for MD5 stores              | Preserves dev data                              | Code complexity for data the team already labeled disposable      | User confirmed existing stores do not matter             |
| Defer the config file to a later ADR                 | Smaller ADR                                     | Hash-selection landing in clap-only would entrench the flag soup  | Config file is the natural forcing function here         |

---

## Consequences

### Positive
- Stronger default hash. No more "we ship MD5" footgun.
- BLAKE3 is generally faster than MD5 on x86_64, which improves the hot
  ingestion path.
- Per-store header makes the hash choice self-documenting and rules out
  silent mismatches.
- Config file gives us a real surface to extend (TLS, additional listeners,
  multi-namespace wiring) without bloating clap.

### Negative
- More moving parts in `SharedBlockStore`: header read on open, hasher
  carried by reference, width-dependent buffers in hot paths.
- Two hash implementations live in the codebase: BLAKE3 for storage, MD5
  for S3 ETag. Reviewers must keep them straight.
- Operators get a new failure mode: "wrong hash for this store" on
  binary/store mismatch (mitigated by clear error text).

### Risks
- Truncated BLAKE3-128 is still vastly larger than the universe of blocks
  we will ever store, but downstream auditors may flag "truncation"
  without context. Mitigation: document the choice in the README and link
  this ADR.
- Header corruption would brick the store. Mitigation: write the header
  with the same durability as block writes, validate magic+version on
  every open, and back up the header bytes inside a sidecar file at
  creation time.
- Forgetting to thread the hasher through one code path produces a hash
  mismatch on read. Mitigation: route all block-hash computation through
  `SharedBlockStore::hasher()` (no free `Md5::digest` calls in the block
  path) and assert this in tests.

---

## Implementation Plan

1. Introduce `Hasher` trait, `Blake3` impl with width 16/32, and unit tests
   covering both widths.
2. Define `StoreHeader`, write/read it from `SharedBlockStore::new` /
   `::open`. Reject unknown or unsupported headers.
3. Thread the `Hasher` reference through the block-producing call sites
   listed above. Remove the standalone `Md5::digest` calls in the block
   path. Keep MD5 only behind the S3 ETag helpers.
4. Add `TfstorConfig` (serde) and the loader. Make each binary's clap
   struct merge with the loaded config.
5. Update `inspect`, `check`, and `retrieve` tools to read the store
   header before doing any hashing.
6. Smoke tests:
   - Create a Blake3-16 store, write objects, read them back.
   - Create a Blake3-32 store, same.
   - Open a store with a binary built from this ADR after deleting all
     existing dev stores: should succeed.
   - Attempt to open a store with a mocked unknown algo discriminant:
     should refuse with a clear error.
7. Update the README to state BLAKE3 is the default and explain the width
   choice.

---

## Open Questions

- [ ] Default `hash.width` when the operator does not specify: 16 or 32?
  Working preference: 16, on the grounds that 128-bit dedup collisions are
  not a real risk and the smaller metadata keys are worth it. Revisit if
  someone shows a workload where the extra bits matter.
  **Caveat under the adversarial framing added 2026-07-30:** accidental
  collisions at 128 bits are indeed a non-issue, but an *adversarial*
  birthday search against a 128-bit address costs ~2^64 hash evaluations,
  which is expensive-but-imaginable for a well-resourced attacker, and a
  manufactured dedup collision here is exactly the cross-tenant
  substitution this amendment records. For deployments with untrusted
  tenants sharing a block store, the 32-byte width should be the
  recommended (possibly enforced) choice; 16 remains fine for
  single-tenant or trusted-tenant stores.
- [ ] Should the store header include a salt, so two operators with the
  same blocks do not produce identical addresses across deployments? Adds
  privacy at the cost of cross-store dedup, which today is not a feature
  we offer anyway.
- [ ] Config file location convention for the system-wide path
  (`/etc/tfstor/tfstor.toml`) once we have a packaging story.
