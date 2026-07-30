# ADR 0002 Implementation Plan

**Status: implemented** (2026-07-30, branch `development`, commits
`8ed2506`..`69c726e`). This file is a historical record of what was planned;
what was actually built, including where it diverged, is recorded in the
"As implemented" section of docs/adr/0002-blake3-hash-migration.md.

Implements docs/adr/0002-blake3-hash-migration.md: BLAKE3 block addressing,
versioned store header, fixed-width on-disk format (v1), and the TOML config
file. Written as self-contained component specs; sequencing and risk at the
end. Each component states its acceptance criteria so it can be verified
independently.

Decisions settled 2026-07-30 (to be folded into the ADR amendment):

- Default hash width: 32 bytes (BLAKE3 native). 16 remains a per-store
  option for trusted-tenant stores.
- No salt / keyed hashing. Header reserved bytes leave room to add it in a
  later version.
- Naming: qss_storage everywhere -- qss_storage.toml, /etc/qss_storage/,
  header magic "QSST". The ADR's tfstor naming is stale.
- No migration from existing stores (per ADR decision 5); unknown or missing
  headers refuse cleanly.

Findings from code exploration that correct the ADR text:

- There is exactly ONE block-addressing hash site:
  cas-storage/src/cas/write_path.rs:117-119. The ADR's threading list is
  wrong: multipart.rs and block_stream.rs compute no hashes, and
  read_path.rs has no verification today (verify-on-read is net-new).
- Object.hash (metastore/object.rs:30) and MultiPart.hash
  (cas/multipart.rs:15) are typed BlockID but hold MD5 content hashes (the
  S3 ETag source). The type split below must land before any width change,
  or ETags silently become 64 hex chars.
- md-5 stays a cas-storage dependency: the ETag hash is computed inside
  cas-storage (write_path.rs:86, :232, :266), not in the frontends.
- calculate_multipart_hash (s3cas/src/s3fs.rs:59-76) hashes raw block IDs,
  contradicting its own "Md5 of the Md5 of the parts" comment. This plan
  fixes it to the S3 convention. Deliberate behavior change, covered by the
  no-compat stance.
- The ADR's "verify full block content on every dedup hit" was the interim
  mitigation for MD5. It is NOT implemented here: BLAKE3-32 makes
  manufactured collisions infeasible, and width 16 is documented as
  trusted-tenant-only. Verify-on-read (opt-in, default off) lands instead;
  it is the only bitrot detection the system has.

---

## Component 1: ContentHash type

New type in cas-storage/src/metastore/ (object.rs or a small new module):

    pub struct ContentHash(pub [u8; 16]);

Documented as: MD5 content digest, S3 ETag source; never a block address.

Replaces BlockID on:

- Object.hash (metastore/object.rs:30)
- MultiPart.hash (cas/multipart.rs:15)
- store_object / store_inlined_object returns (cas/write_path.rs)
- s3cas/src/s3fs.rs ETag plumbing, s3cas/src/check.rs:63-68
- respd/src/namespace.rs:246 (SET hash), :340-341 (CHECK)

Two fixes ride along because they touch the same lines:

- calculate_multipart_hash (s3fs.rs:59-76) becomes MD5 over the
  concatenation of the per-part ContentHashes (each part's MD5, already
  stored in MultiPart.hash via write_path.rs:86 -> s3fs.rs:702-711), ETag
  "{hex}-{N}" with N = part count. Needs hash()/size() getters on
  MultiPart.
- The empty-object sentinel [0; 16] (write_path.rs:246) becomes
  Md5::digest(b"") so empty objects get the standard d41d8cd9... ETag.

Acceptance:

- No remaining site assigns a block address into an ETag field or vice
  versa; the compiler enforces this (grep for BlockID in s3fs.rs ETag paths
  returns nothing).
- it_s3.rs multipart test passes with an ETag asserting 32 hex + "-N".
- Empty-object PUT returns ETag d41d8cd98f00b204e9800998ecf8427e.

## Component 2: BlockId type

Replace `pub type BlockID = [u8; BLOCKID_SIZE]` (metastore/block.rs:15)
with a runtime-width newtype (const generics cannot express per-store
runtime width):

    pub struct BlockId { bytes: [u8; 32], len: u8 }   // Copy

Invariant: bytes[len..] is always zero, enforced in constructors, so
derived PartialEq/Eq/Hash/Ord are correct without manual impls.

API: from_slice(&[u8]) -> Result (len must be 16 or 32), as_slice(),
len(). During this component's landing, all construction stays MD5/16-byte;
only the type changes.

Call sites to update: Vec<BlockID> in object.rs and multipart.rs, pattern
matches, stores/test_utils.rs BlockID::from([1;16]) x6,
benches/fjall_benchmark.rs:56,68,75, s3fs.rs:59, BlockTree::iter_all
(meta_store.rs:482-502, start bound becomes width-sized), and the prefix
path loop (meta_store.rs:596-613): bound becomes 1..hash.len(), and the
latent exhaustion bug is fixed -- when every prefix is taken, fall back to
the full-width path instead of the current empty path (which panics in
Block::disk_path).

Acceptance:

- Unit test: two BlockIds are equal iff their as_slice() are equal;
  padding invariant holds for both widths.
- Prefix-exhaustion test: inserting colliding-prefix blocks never produces
  an empty path.

## Component 3: On-disk record format v1

All length/count fields become u64 LE; PTR_SIZE (metastore/constants.rs:4)
is deleted. In-memory usize fields (Block.size/rc, MultiPart.size,
ObjectData::MultiPart.parts, write_block data_len) convert at the boundary
with checked try_into.

Records containing block-ID lists serialize a self-describing width byte so
TryFrom<&[u8]> stays context-free (tools can parse records standalone; no
store-width threading into deserializers). id_width is 16 or 32; 0 is valid
only when count == 0.

Layouts (all integers LE):

Block (block.rs):

    size u64 | path_len u8 | path[path_len] | rc u64

BucketMeta (bucket_meta.rs):

    ctime i64 | name_len u64 | name[name_len]

Object (object.rs):

    type u8 | size u64 | ctime i64 | hash [16]
    Inline:     data_len u64 | data          (fixes the :316 write-u64 vs
                                              :405 read-PTR_SIZE mismatch)
    SinglePart: id_width u8 | count u64 | ids[count * id_width]
    MultiPart:  parts u64 | id_width u8 | count u64 | ids

MultiPart part record (cas/multipart.rs):

    size u64 | part_number i64 | bucket_len u64 | bucket | key_len u64 |
    key | upload_len u64 | upload_id | hash [16] |
    id_width u8 | count u64 | ids

Every record gets an exact-length check. This specifically replaces the
MultiPart chunks_exact loop that today absorbs trailing garbage as extra
blocks.

Inline threshold: minimum_inline_metadata_size (object.rs:132-134) becomes
a constant again (Inline objects carry no block list): 1+8+8+16+8 = 41.
Verify max_inlined_data_length (meta_store.rs:51-56) and the gate at
s3fs.rs:631 against it.

Error surface (lands BEFORE the format change so format bugs surface as
errors, not panics):

- FsError grows detail variants replacing the lone MalformedObject:
  Truncated { record, needed, got }, TrailingBytes { record, extra },
  InvalidUtf8 { record, field }, UnknownObjectType(u8),
  InvalidIdWidth(u8), LengthOverflow { record, field }.
- New MetaError::Corruption(FsError) + From<FsError> for MetaError.
- The seven deserialize panic sites become Result-propagating:
  meta_store.rs:241, :288, :305, :421; cas/multipart.rs:203;
  stores/fjall_common.rs:370; respd/src/namespace.rs:257. The silent drop
  at meta_store.rs:200 (filter_map .ok()) propagates too.

Acceptance:

- Golden byte-layout fixtures for all four record types, both id widths,
  and count == 0, committed in the same change as the format: serialize ->
  expected bytes, expected bytes -> deserialize.
- Malformed-input tests per record: truncated, trailing bytes, bad
  discriminant, bad id_width -- each returns the matching FsError variant,
  no panics.
- Full workspace suite green; manual aws CLI multipart round-trip against a
  fresh store.

## Component 4: Hasher

New cas-storage/src/hasher.rs (top level; used by both cas and metastore
layers). Concrete enum, not the ADR's trait -- block hashing is one-shot
over a fully buffered 1 MiB chunk, so no StreamingHasher and no SmallVec:

    pub enum Hasher { Blake3W16, Blake3W32 }   // Copy
    impl Hasher {
        pub fn algo_id(&self) -> u8;           // Blake3 = 1 (both widths)
        pub fn width(&self) -> u8;             // 16 | 32
        pub fn hash(&self, data: &[u8]) -> BlockId;
        pub fn from_header(algo: u8, width: u8) -> Result<Self, ...>;
    }

blake3 added via cargo add to [workspace.dependencies] + cas-storage.

Acceptance:

- Official BLAKE3 test vectors pass for both widths.
- Truncation-is-prefix property: W16 output == first 16 bytes of W32.
- Returned BlockIds satisfy the zero-padding invariant.

## Component 5: StoreHeader

Lives at MetaStore level so respd's store (which never touches
SharedBlockStore) gets format versioning too. hash fields are written
everywhere but consulted only by SharedBlockStore.

New cas-storage/src/metastore/store_header.rs; fixed 32-byte serialization
stored in a _STORE_HEADER partition under a single fixed key:

    magic [4] = "QSST" | version u16 = 1 | hash_algo u8 | hash_width u8 |
    created_at u64 (unix secs) | reserved [16] (zeroed)

Semantics:

- MetaStore construction becomes open_or_create(store, inlined_size, spec)
  -> Result. Create-vs-open is decided by checking the db directory before
  opening fjall (fjall creates partitions on open): nonexistent/empty dir
  means create.
- Refusal errors, each with distinct operator-readable text: header missing
  on a non-empty store ("store predates the QSST format; no migration
  exists"), bad magic, unsupported version, unknown algo, unsupported
  width.
- A sidecar backup store_header.bin is written next to the db dir at
  creation (ADR header-corruption mitigation).
- Bucket-name guard added here: bucket names starting with "_" are rejected
  at creation, protecting _STORE_HEADER, _BLOCKS, _PATHS,
  _MULTIPART_PARTS (nothing enforces this today).

Wiring:

- SharedBlockStore::new (shared_block_store.rs:29-72) splits into
  create/open, exposes hasher() built via Hasher::from_header. The hasher
  is constructed at open even before the write path uses it, so mismatch
  refusal is live immediately.
- CasFS::new namespace store (fs.rs:59-99). Note single_namespace creates
  TWO headered DBs (blocks DB + namespace DB).
- respd Storage::new (respd/src/storage.rs:22-31).
- inspect/retrieve/check get minimal plumbing to compile; full tool UX is
  Component 8.

Acceptance:

- Create-then-reopen round-trip test per backend (backend battery is the
  natural home).
- Doctored-header tests: unknown algo, unknown width, bad magic, wrong
  version -- each refuses with its distinct error text.
- Manual: create a store with s3cas, corrupt the header partition, observe
  the clean refusal.

## Component 6: BLAKE3 in the write path, verify-on-read

The switch is one line-region: write_path.rs:117-119 becomes

    let block_hash = fs.shared.hasher().hash(&bytes);

The MD5 sites at :86, :232, :266 are ContentHash/ETag and do not change.
Downstream is already width-agnostic from Components 2-3.

Verify-on-read (opt-in, default off): in the block read path
(cas/read_path.rs), recompute hasher.hash(block_bytes) and compare to the
address; mismatch yields a typed corruption error naming the block. Full
blocks only, no partial-range verification. Plumbed as a constructor
parameter until the config file (Component 7) gives it a home.

Acceptance:

- CAS test matrix in fs.rs extends from TEST_ENGINES to
  TEST_ENGINES x [W16, W32]; all green.
- End-to-end assertion at both widths: a written block's on-disk file
  content re-hashes to its address (guards against a missed hash site).
- it_s3.rs green with UNCHANGED ETags relative to Component 1 -- proves the
  ETag/block-hash separation held.
- grep -ri md5 cas-storage/src shows only the ETag sites.
- Verify-on-read test: flip a byte in a stored block file, read with the
  flag on -> corruption error; flag off -> bytes served.

## Component 7: Config file

New cas-storage/src/config.rs; serde + toml added to
[workspace.dependencies].

    QssStorageConfig {
        store: StoreConfig {
            hash: { algo: "blake3", width: 32 },   // default width 32
            durability: "buffer"|"fsync"|"fdatasync",
            inline_metadata_size: Option<usize>,
            metadata_db: "fjall"|"fjall_notx",
            verify_on_read: bool,                  // default false
        },
        s3: Option<S3Config { host, port, access_key, secret_key,
                              metrics: { host, port } }>,
        resp: Option<RespConfig { host, port, data_dir, admin_password }>,
    }

Precedence: --config <path> > ./qss_storage.toml >
/etc/qss_storage/qss_storage.toml > built-in defaults. CLI flags override
config fields.

Details:

- Durability and StorageEngine get Deserialize via
  #[serde(try_from = "String")] over their existing FromStr, plus Display
  for round-tripping.
- store.hash applies at store creation only. On open, the header wins; a
  config/header mismatch logs a warning (the header is immutable per
  ADR 0001).
- s3cas: all ServerConfig fields become Option<T> WITHOUT default_value
  (clap cannot otherwise distinguish user-passed from default);
  required = true dropped from access/secret key -- startup error only if
  neither CLI nor config supplies them; --config flag added. The
  inspect/retrieve/check subcommands stop hardcoding None for
  durability/inline size and share the loaded config.
- respd: same Option-ification; un-hardcode inline size (main.rs:51,
  built-in default stays 1) and durability (storage.rs:26, default fsync).
- Metrics section is consumed only by s3cas; the single
  SharedMetrics::new()-per-process constraint (global prometheus registry,
  metrics.rs:110) is unchanged.

Acceptance:

- Unit tests: precedence order, partial-file merge, unknown-key rejection
  (deny_unknown_fields), Durability/StorageEngine round-trip.
- Manual matrix: no config file (defaults), config only, config + CLI
  override -- verified for at least port, durability, width.
- `s3cas server` starts from a config file alone with no flags (fixes the
  Makefile run-s3cas ergonomics gap).

## Component 8: Tools

- inspect (s3cas/src/inspect.rs): open via the header-aware path AND fix
  the existing bug where it does not append db/ the way CasFS does
  (inspect.rs:11-20) -- today it opens a different DB than the server
  writes. New `header` subcommand printing magic/version/algo/width/
  created_at.
- check: block hashes verified with the store's hasher; object
  hash (ETag) verified with MD5, per the ADR split.
- retrieve: opens via header; no behavior change otherwise.

Acceptance:

- `s3cas inspect header` on a fresh store prints QSST v1 blake3/32.
- `s3cas inspect num-keys` agrees with what the server wrote (regression
  for the db/ path fix).
- `s3cas check` passes on a healthy store, fails on a byte-flipped block.

## Component 9: Docs and ADR amendment

- ADR 0002: Status -> Accepted. Naming to qss_storage/QSST. Width default
  resolved to 32 (16 = trusted-tenant option). Corrected hash-site list
  (one site). As-implemented Hasher signature (enum returning BlockId; no
  SmallVec, no StreamingHasher). Record: no dedup-hit verification +
  verify-on-read opt-in rationale; md-5 stays in cas-storage for the ETag
  path; salt question closed (reserved bytes); multipart ETag fixed to S3
  convention.
- docs/as-built/02-storage-model.md: v1 layouts, header, width byte,
  verify-on-read.
- docs/as-built/04-code-health.md resolution table: H3 -> Fixed (this
  work); H7 stays open unless picked up separately.
- cas-storage/EXTENSIONS.md: new/changed extension regions (multipart.rs,
  block-list serialization).
- README: BLAKE3 default, width guidance (32 default; 16 trusted-tenant),
  config file usage.

---

## Sequencing

Order is forced by type safety: the ContentHash/BlockId split (1, 2) must
precede the format change (3), which must precede the hash switch (6).

1. Component 3's error surface (typed FsError variants, panic-site
   conversion) -- lands first inside Component 3 work.
2. Component 1 (ContentHash + ETag fixes).
3. Component 2 (BlockId newtype).
4. Component 3 (format v1 + golden vectors, one commit).
5. Component 4 (Hasher, inert).
6. Component 5 (StoreHeader + wiring).
7. Component 6 (the switch + verify-on-read).
8. Component 7 (config).
9. Components 8, 9 (tools, docs).

Roughly 14 commits; the tree stays fmt + clippy -D warnings + test green
after each. Standing gate: cargo fmt --check; cargo clippy --workspace
--all-targets -- -D warnings; cargo test --workspace (respd integration
and it_s3 single-threaded per Makefile).

Release-train note: between the format change (Component 3) and the header
(Component 5), an old store opens as garbage -- corruption errors rather
than a clean refusal. Acceptable on development; do not cut a release
inside that window.

## Risk ranking

1. Format v1 rewrite (Component 3): byte-offset arithmetic across four
   record types; errors are silent data-shape bugs. Mitigated by typed
   errors landing first, the type split landing first (the compiler
   polices which 16-byte value is which), golden vectors in the same
   commit, and exact-length checks on every record.
2. The hash switch (Component 6): mitigated by the disk-content-rehash
   test at both widths and the ETag-stability assertion.
