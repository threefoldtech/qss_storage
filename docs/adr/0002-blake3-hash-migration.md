# BLAKE3 Hash Migration and Introduction of a Config File

**Status**: Accepted
**Date**: 2026-05-26
**Amended**: 2026-07-30 -- recorded the multi-tenant substitution risk
(making this migration a prerequisite for untrusted multi-tenancy, not an
improvement), folded fixed-width on-disk fields into the same format break,
and flagged the width default under the adversarial framing.
**Implemented**: 2026-07-30 -- landed on branch `development` in the
fourteen-step pass specified in
[docs/plans/adr-0002-implementation.md](../plans/adr-0002-implementation.md),
commits `8ed2506`..`69c726e`.
**Amended**: 2026-07-30 (as-built pass) -- naming corrected from `tfstor` to
`qss_storage`, the three open questions resolved, and the architecture
section reconciled with what was actually built. The earlier text stands;
where the build diverged from it, the divergence is recorded in bracketed
*as implemented* notes and collected under
[As implemented](#as-implemented-2026-07-30) rather than edited away.

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
  *[As implemented: dedup-hit verification was not built. It was the interim
  mitigation for MD5 and the migration removed its reason to exist -- the
  default BLAKE3-32 address makes a manufactured dedup collision infeasible.
  The 16-byte width, which is the one that could conceivably be attacked, is
  documented as trusted-tenant-only instead. See
  [As implemented](#as-implemented-2026-07-30).]*
- BLAKE3 is faster than MD5 on modern CPUs (SIMD, parallelism) AND offers
  256-bit collision resistance.
- The hash is baked into every block address. Inside one store it must be a
  single, immutable choice (ADR 0001, Resolved).

Operational state when this ADR is written:

- Both binaries are configured only via clap CLI flags
  (`s3cas/src/main.rs:25`, `respd/src/main.rs:16`). No config file exists.
- Existing qss_storage stores are throwaway development data. Backwards
  compatibility with MD5-addressed stores is **explicitly not a goal**.
- MD5 is still required by the S3 protocol for ETag computation
  (`s3cas/src/s3fs.rs:58`, `s3cas/src/check.rs:63`). This is independent
  of the storage hash and must be preserved.
  *[As implemented: the ETag digest is computed inside `cas-storage`
  (`cas/write_path.rs`), not in the frontends, so `md-5` stays a
  `cas-storage` dependency rather than moving out to `s3cas`.]*

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
   *[As implemented: the default is 32; see the resolved open question
   below.]*
4. **Introduce a TOML config file** as the primary configuration surface.
   CLI flags remain as overrides. The config file is the natural home for
   hash selection, durability, inline threshold, ports, and credentials.
   *[As implemented: `qss_storage.toml`, parsed into `QssStorageConfig`
   (`cas-storage/src/config.rs`), documented key by key in
   `qss_storage.toml.example`.]*
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
   *[As implemented at `a0c6471`, format v1, exactly as decided. One
   addition the decision did not anticipate: because the block address is
   now a per-store width, every record that carries a block-id list also
   carries a one-byte `id_width`, which keeps `TryFrom<&[u8]>`
   context-free -- a tool can decode a record without being told which
   store wrote it.]*

---

## Architecture Overview

### Hasher

Sketched here as a trait:

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

*[As implemented (`cas-storage/src/hasher.rs`, `43d6594`): a concrete enum,
not a trait, returning the `BlockId` newtype directly.*

```
pub enum Hasher { Blake3W16, Blake3W32 }   // Copy
impl Hasher {
    pub fn algo_id(&self) -> u8;                       // Blake3 = 1
    pub fn algo_name(&self) -> &'static str;           // "blake3"
    pub fn width(&self) -> u8;                         // 16 | 32
    pub fn hash(&self, data: &[u8]) -> BlockId;
    pub fn from_header(algo: u8, width: u8) -> Result<Self, HasherError>;
}
```

*Three reasons the trait shape was dropped. First, there is no streaming to
abstract: a block hash is one-shot over a fully buffered 1 MiB chunk, so
`StreamingHasher` had no caller. The only streaming hash in the system is
the S3 ETag digest, and that one stays MD5 by protocol obligation, so it
could never have shared this abstraction anyway. Second, `SmallVec` bought
nothing once `BlockId` existed: it is a `Copy` newtype of `[u8; 32]` plus a
length, so the hasher can return the storage type itself instead of a
buffer someone has to convert. Third, an enum makes width-sensitive sites
(serialization, header, tools) fail to compile when a variant is added,
where a trait object would silently take a default branch; and it costs no
dynamic dispatch on the write path. MD5 is deliberately not a variant:
giving it an `algo_id` would make a collision-broken function selectable as
a content address.]*

### Store header

Each `SharedBlockStore` writes a `_STORE_HEADER` record on creation:

```
struct StoreHeader {
    magic: [u8; 4],         // "QSST"
    version: u16,           // start at 1
    hash_algo: u8,          // Blake3 = 1
    hash_width: u8,         // 16 or 32
    created_at: u64,        // unix seconds
    reserved: [u8; 16],
}
```

On open: read the header, instantiate the matching `Hasher`, and store it
inside `SharedBlockStore`. Mismatched / unknown hashers abort cleanly.

*[As implemented (`cas-storage/src/metastore/store_header.rs`, `03c5cf5`):
the layout above, serialized to exactly 32 bytes, but the header lives one
layer lower than this section says. It is written and validated at
`MetaStore` level, not `SharedBlockStore` level, so **every** fjall
database this codebase creates carries one: the shared block DB, each
namespace DB (`CasFS::single_namespace` therefore creates two headered
DBs), and respd's key-value DB, which never touches a block. The hash
fields are written everywhere and consulted only by `SharedBlockStore`;
what the other stores get out of it is format versioning. The record sits
in a `_STORE_HEADER` partition under a single fixed key, and a byte-for-byte
sidecar copy, `store_header.bin`, is written next to the db directory at
creation -- that is the ADR's own header-corruption mitigation, made
concrete. Create-versus-open is decided by inspecting the db directory
before fjall is allowed to open (and thereby create) anything.
Bucket names starting with `_` are refused at creation so a bucket can
never collide with `_STORE_HEADER`, `_BLOCKS`, `_PATHS` or
`_MULTIPART_PARTS`.]*

### Threading the hasher

`Hasher` is owned by `SharedBlockStore`, then referenced by every code
path that produces a content address:

- `cas-storage/src/cas/write_path.rs` - block hash, inline hash.
- `cas-storage/src/cas/multipart.rs` - per-part hash.
- `cas-storage/src/cas/block_stream.rs` - block boundary hashing.
- `cas-storage/src/cas/read_path.rs` - verification on read.
- `s3cas/src/check.rs` - integrity check tool (block hash via store, ETag
  via fixed MD5).

*[As implemented: this list was wrong when it was written, and the code
exploration for the implementation plan corrected it. There is exactly
**one** site that turns block bytes into a block address:*

- *`cas-storage/src/cas/write_path.rs` -- the per-chunk block hash. This
  one line region is the whole migration (`23ed542`). The MD5 calls that
  remain in the same file are the streaming `ContentHash`/ETag digest and
  are untouched by the width.*
- *`cas-storage/src/cas/multipart.rs` and
  `cas-storage/src/cas/block_stream.rs` computed no hashes at all, before
  or after. They carry block ids around; they do not produce them.*
- *`cas-storage/src/cas/read_path.rs` had no verification to thread a
  hasher into: read verification is net-new here, shipped as the opt-in
  `verify_on_read` (`BlockStream::verified`), which re-hashes whole blocks
  only. A range request cannot be checked against a whole-block address
  and is documented as unverified.*
- *`s3cas/src/check.rs` is as described: block files re-hashed with the
  store's hasher, object ETag compared with MD5 (`69c726e`).]*

### Config file

New crate-level type:

```
struct QssStorageConfig {
    store: StoreConfig {
        hash: HashConfig { algo: "blake3", width: 16 | 32 },
        durability: "buffer" | "fsync" | "fdatasync",
        inline_metadata_size: Option<usize>,
        metadata_db: "fjall" | "fjall_notx",
        verify_on_read: bool,                    // default false
    },
    s3: Option<S3Config { host, port, access_key, secret_key,
                          metrics: { host, port } }>,
    resp: Option<RespConfig { host, port, data_dir, admin_password }>,
}
```

Loader order:
1. `--config <path>` if given.
2. `./qss_storage.toml` if present.
3. `/etc/qss_storage/qss_storage.toml`.
4. Built-in defaults.

CLI flags override the equivalent config-file field. Each binary still
parses its own clap struct; the struct's defaults are populated from the
loaded config.

*[As implemented (`cas-storage/src/config.rs`, `fdd0d97`): the shape above,
with `verify_on_read` added because Component 6 needed a home for it, and
`metrics` nested under `[s3]` because only s3cas serves it. Clap fields
became `Option<T>` with no `default_value`, which is the only way to tell a
flag the user passed from one clap invented; a `--config` path that does
not exist is a startup error rather than a fall-through; and an unknown key
anywhere in the file is a startup error, on the grounds that a storage
daemon silently ignoring a misspelled setting is worse than one that
refuses to start. `[store.hash]` is consulted only when a store is created:
on open the header wins and a disagreement is logged as a warning, since
the blocks are already addressed by what the header says.]*

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

*[As implemented: no `streaming()` call -- each 1 MiB chunk is buffered and
hashed in one shot by `SharedBlockStore::hasher().hash(&bytes)`. The MD5
ETag digest is the one that streams, fed chunk by chunk in the same loop, so
"in parallel" is accurate. Addresses are plain BLAKE3, not keyed; see the
salt question below.]*

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
4. Add `QssStorageConfig` (serde) and the loader. Make each binary's clap
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

*[As implemented: the seven steps became fourteen, specified as
self-contained component specs in
[docs/plans/adr-0002-implementation.md](../plans/adr-0002-implementation.md).
Three things were inserted ahead of the hash switch that this plan does not
mention, all of them forced by type safety: typed decode errors so a format
bug surfaces as an error rather than a panic (`8ed2506`), the
`ContentHash`/`BlockId` split (`7fcc191`, `c6991e3`), and format v1 itself
(`a0c6471`). The split had to land first because `Object.hash` and
`MultiPart.hash` were typed as block ids while holding MD5 ETag digests --
without separating them, widening the block address would have silently
turned every S3 ETag into 64 hex characters.]*

---

## As implemented (2026-07-30)

What landed on `development`, in commit order. Each item is the delta
between this ADR as written and the code as built; anything not listed here
was built as specified.

| Step | Commit | What |
|------|--------|------|
| C1 | `8ed2506` | Typed decode errors; the seven deserialize panic sites propagate instead |
| C2 | `7fcc191` | `ContentHash` (MD5/ETag) split from the block-address type |
| C3 | `c6991e3` | `BlockId` newtype carrying its width at runtime |
| C4 | `a0c6471` | On-disk format v1: `u64` fields, `id_width` byte, golden vectors |
| C5 | `43d6594` | `Hasher` enum (BLAKE3, both widths), pinned to the official test vectors |
| C6-C7 | `03c5cf5` | QSST store header; unheadered or mismatched stores refuse to open |
| C8-C9 | `23ed542` | Blocks become BLAKE3-addressed; opt-in verify-on-read |
| C10-C11 | `fdd0d97` | `qss_storage.toml` and the loader |
| C12-C13 | `69c726e` | Header-aware `inspect`/`check`/`retrieve`; width benchmark |

Substantive divergences and additions:

- **Hasher is a concrete enum, not a trait.** No `StreamingHasher`, no
  `SmallVec`. Rationale in the architecture section above; the short form is
  that block hashing is one-shot over a buffered chunk, and the only
  streaming hash in the system is the S3 ETag, which stays MD5.
- **One block-hash site, not five.** The threading list in this ADR was
  inaccurate. Corrected inline above.
- **The store header sits at `MetaStore` level**, so respd's store and every
  namespace DB are format-versioned too, not just the shared block store.
  32 bytes, `_STORE_HEADER` partition, `store_header.bin` sidecar.
- **No dedup-hit content verification.** It was this ADR's interim
  mitigation for shipping MD5, and the migration retires the reason for it:
  at the default 32-byte address a manufactured dedup collision is not
  reachable. The 16-byte width is the one where the argument could be made,
  and it is handled by documenting it as a trusted-tenant option rather than
  by paying a full block read on every dedup hit. What did ship instead is
  `verify_on_read` (opt-in, default off, whole blocks only): not a
  substitution defence but the only bitrot detection the system has.
- **`md-5` remains a `cas-storage` dependency.** The ETag digest is computed
  in the write path inside the library, not in the frontends, so removing
  MD5 from `cas-storage` would have meant moving the ETag computation out of
  it. `grep -i md5 cas-storage/src` returns only `ContentHash`/ETag sites.
- **The multipart ETag was wrong and is now right.** `calculate_multipart_hash`
  hashed raw block ids, contradicting its own "MD5 of the MD5s" comment. It
  now implements the S3 convention: MD5 over the concatenated per-part MD5s,
  rendered `{hex}-{N}` with N the part count. The empty-object sentinel
  changed from sixteen zero bytes to the standard empty-body MD5
  `d41d8cd9...`. Both are deliberate behaviour changes, covered by decision 5
  (no compatibility with existing stores).
- **Records carry an `id_width` byte** so a block-id list decodes without
  knowing which store wrote it (decision 6, extended).
- **Width has no measurable write-path cost.** The `casfs_benchmark`
  W16-vs-W32 group (`69c726e`) shows no measurable difference, which makes
  the width a metadata-size choice rather than a throughput one. That is the
  number behind the operator guidance.

---

## Open Questions (resolved 2026-07-30)

- [x] Default `hash.width` when the operator does not specify: 16 or 32?
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
  **Resolved: the default is 32.** The working preference above traded
  strength for smaller metadata keys, and the benchmark removed the other
  half of the trade -- the W16-vs-W32 write-path group shows width is not
  measurable on the hot path, so 16 buys 16 bytes per block id in the
  metadata records and nothing else. Paying that to keep a default that is
  only safe inside a trust boundary is the wrong way round for a default.
  16 stays available per store for trusted-tenant deployments that care
  about metadata size, and is documented as exactly that in the README and
  in `qss_storage.toml.example`.
- [x] Should the store header include a salt, so two operators with the
  same blocks do not produce identical addresses across deployments? Adds
  privacy at the cost of cross-store dedup, which today is not a feature
  we offer anyway.
  **Resolved: not implemented.** Addresses are plain, unkeyed BLAKE3. No
  concrete requirement asked for the privacy property, and adding a keyed
  hash now would mean a key-management story (where the key lives, what
  happens when it is lost, how a store is reopened without it) for a
  benefit nobody has requested. The door is deliberately left open: the
  header's 16 reserved bytes are written zeroed and round-trip untouched,
  so a future version can define a salt or key id there. Changing the
  meaning of an existing field would require a version bump instead.
- [x] Config file location convention for the system-wide path
  (`/etc/qss_storage/qss_storage.toml`) once we have a packaging story.
  **Resolved as implemented:** `/etc/qss_storage/qss_storage.toml`, third
  in the precedence chain after `--config <path>` and `./qss_storage.toml`,
  ahead of the built-in defaults. Each binary logs at startup which file it
  loaded, or that it found none.
