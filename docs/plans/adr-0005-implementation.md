# ADR 0005 Implementation Plan: fsck / scrub / reconciliation

Executes `docs/adr/0005-fsck-scrub-reconciliation.md` (decision-complete
2026-07-31). Seven components, one gate-green commit each, in order.
Written against HEAD f35e2a8 plus the two ADR 0005 doc commits; every
file:line below was verified against that tree.

## Hard rules (apply to every component)

1. **Closed holder set.** The recount's holder enumeration must be
   complete or the recount (and all repair) refuses to run. Holder
   trees are enumerated from the namespace DB itself (new
   `Store::list_trees`, component 2), NOT from `_BUCKETS` rows -- a
   half-deleted bucket's surviving object tree still holds refcounts
   and `_BUCKETS` no longer names it. Walkers use `iter_all`
   (per-item `Result`) and treat any undecodable holder record as a
   hard refusal; never `range_filter`, which silently skips
   undecodable records (`fjall.rs:375-409`).
2. **Report before repair; repair idempotent; recount after repair.**
   The report is fully emitted before the first repair action runs.
   Every action tolerates re-application (set-rc, ENOENT-tolerant
   unlink/rename, idempotent flag-set). After all actions, the recount
   re-runs; anything but a clean diff is CRITICAL, exit 2.
3. **Loss-never in repair.** rc is only ever set to the walked truth
   under a complete enumeration. Raising is allowed (leak direction).
   Adoption (pointing a record at an orphan file) requires re-hashing
   the orphan first -- fsck does not have the writer's bytes.
4. **Quarantine, never delete**, for corrupt blocks and foreign files:
   rename into `blocks/.quarantine/`. Orphans and off-depth duplicates
   (nothing references them) may be deleted.
5. **fjall stays the leaf lock; no await inside a tx** (ADR 0006
   invariants). The only daemon-path change in this plan is component
   1's degraded branch, which lives inside the existing tx and stripe.
6. Plain ASCII in all files. Gates per commit: `make fmt`,
   `cargo clippy --workspace --all-targets -- -D warnings`,
   `cargo test --workspace`. One commit per component, message
   naming the component and the ADR.

## Component 1: record format v3 -- the degraded flag

The daemon-side foundation; everything else is offline tooling.

**block.rs** (`cas-storage/src/metastore/block.rs`):
- `Block` gains `flags: u8` (private, like the rest). Bit 0 =
  degraded; other bits reserved-zero. Wire format v3 =
  `size u64 LE | depth u8 | rc u64 LE | flags u8` = 18 bytes exactly
  (encoder at `block.rs:175-184`, decoder `:187-199`; `Reader::finish`
  keeps rejecting trailing bytes, so v2's 17-byte records fail decode
  -- correct: the store header version gates the break).
- `Block::new(size, depth)` keeps its signature, `flags: 0`.
- New accessors: `is_degraded(&self) -> bool`;
  `pub(crate) fn set_degraded(&mut self, bool)`;
  `pub(crate) fn from_parts(size, depth, rc, flags) -> Block` (repair
  and fixtures need to construct arbitrary records; keep it
  crate-internal).
- Regenerate the golden vectors (`GOLDEN_DEPTH_2`/`GOLDEN_DEPTH_MAX`,
  `block.rs:349-367`) and the truncation/trailing tests for 18 bytes;
  add a degraded-bit round-trip vector.

**store_header.rs**: `STORE_HEADER_VERSION: u16 = 3`
(`store_header.rs:64`) with doc comment naming ADR 0005. Touches: the
create stamp (`:141`), the equality gate (`:202`), the message
(`:264`), the golden header vector (`:398-405`, regenerate bytes and
comment), `rejects_the_previous_version` (`:490-495`, now rejects 2),
and the s3cas test assert (`s3cas/src/inspect.rs:300`, no change
needed -- it compares against the constant).

**meta_store.rs**:
- `Transaction::bump_block_rc` (`meta_store.rs:487-507`): after the
  decode at `:494`, if `block.is_degraded()` return `Ok(None)` --
  degraded is absent-for-dedup. The write path's match
  (`write_path.rs:101-118`) already falls through to the insert path
  on `Ok(None)`; it needs no change.
- `Transaction::insert_new_block` (`meta_store.rs:580-598`) becomes
  heal-aware: inside the tx, `get` the record first. Absent: insert
  `Block::new` (rc=1) as today. Present: it can only be a degraded
  record (only stripe holders insert, and our own bump returned None
  under this stripe hold) -- clear the flag, set `depth` to the depth
  the file was just written at (the heal's file may land at a
  different depth than the dead record stored; the record must follow
  the file), rc+1, re-insert. Debug-assert `!present.is_degraded()`
  is impossible, and that sizes match (same content, same size).
- `decrement_block_rc` needs no special case: a degraded record
  decrements normally; if it reaches zero the record is removed and
  the unlink tolerates ENOENT (there is no file -- that is what
  degraded means).

**crash_fixtures.rs**: add
`pub(crate) fn plant_degraded_record(shared, id, depth, rc)` using
`Block::from_parts` via a tx (parallel to `plant_dangling_record`,
`crash_fixtures.rs:47-56`), plus a pinning test (record exists,
`is_degraded()`, no file).

**race_tests.rs**: a degraded arm following the house harness
(`store_with_namespaces`, exact-rc asserts): plant a degraded record
rc=N, fire K concurrent same-content PUTs, assert final state: flag
cleared, file present and hash-valid at the record's depth, rc exactly
N+K (one heal + K-1 dedup bumps), no partial file. Run under the storm
iteration pattern (`STORM_ITERATIONS`, `race_tests.rs:24`).

## Component 2: enabling surfaces -- list_trees, StoreOptions move,
   multipart tree constant

- **`Store::list_trees`**: add
  `fn list_trees(&self) -> Result<Vec<String>, MetaError>` to the
  `Store` trait (`traits.rs:111`); `FjallStore` implements via
  `self.db.db.list_keyspace_names()`
  (fjall `SingleWriterTxDatabase::list_keyspace_names`); `TestStore`
  (`stores/test_utils.rs`) gets a trivial impl. Passthrough
  `MetaStore::list_trees` next to `get_tree` (`meta_store.rs:175`).
  Unit test: create buckets, assert the bucket trees and the reserved
  `_`-prefixed trees all appear.
- **StoreOptions moves to cas-storage**: relocate `StoreOptions`,
  `resolve`, `header_spec` (currently `s3cas/src/store_options.rs:15-66`)
  into `cas-storage` (new `cas-storage/src/store_options.rs`,
  re-exported from lib.rs next to `config`); s3cas keeps a thin
  re-export or updates imports. Zero behavior change; s3cas tests keep
  passing untouched. This is what lets the fsck binary share the
  CLI-over-config-over-defaults merge instead of duplicating it.
- **`_MULTIPART_PARTS` constant**: name the literal
  (`shared_block_store.rs:101`) as a `pub const MULTIPART_PARTS_TREE`
  next to `DEFAULT_BLOCK_TREE` (`meta_store.rs:23`) and replace use
  sites.

## Component 3: scrub walkers and the findings model

New `pub mod scrub` in cas-storage (`cas-storage/src/scrub/`,
submodules at the implementer's discretion). All library code, all
synchronous. Inputs: an opened store (the fsck open handles in
component 6) or paths + options.

- **Holder walker**: enumerate namespace-DB trees via `list_trees`,
  filter out reserved `_`-prefixed trees; `iter_all` each; decode
  `Object`; count block references per occurrence via
  `Object::blocks()` (`object.rs:188-194`, exhaustive match). Then
  `iter_all` over `_MULTIPART_PARTS`, decode `MultiPart`, count
  `MultiPart::blocks()` (`multipart.rs:40-42`) the same way. Any
  decode error anywhere = `HolderEnumerationError` carrying tree/key
  context: the closed-holder-set refusal (hard rule 1). Output:
  `HashMap<BlockId, u64>` of expected counts. Blast-radius holder
  lookup for specific damaged blocks is a separate targeted re-walk
  (do not hold a full reverse index in memory).
  A scrub-local pinning test matches exhaustively on `ObjectData` so a
  new variant fails compilation here too (decode dispatches on the
  type byte, so `TryFrom` alone would not catch it --
  `object.rs:331-381`).
- **Record walker**: wrap `BlockTree::iter_all`
  (`meta_store.rs:405-420`) collecting `(BlockId, Block)`; decode
  errors are findings (CRITICAL), not silent skips.
- **Disk walker**: recursive `read_dir` from the blocks root; skip
  `.tmp` (`block_disk.rs:34`) and `.quarantine` at the top level.
  Structure rules: directories must parse as a single hex byte, files
  as full lowercase hex of the store's id width (16 or 32 bytes,
  `hasher.width()`); anything else is a Foreign finding. Yields
  `(BlockId, depth, file_size)` per valid file. Nothing else in the
  tree walks `blocks/` today (verified: only the `.tmp` purge lists a
  directory, and only `.tmp` itself) -- this walker is new surface.
- **Findings model**: `Finding { severity, class, ids/paths, evidence,
  holders }` with `Severity { Info, Warn, Critical }` and a
  `FindingClass` enum covering every class in the ADR. `serde
  Serialize` on all of it (serde is already a cas-storage dep;
  serde_json is already declared and currently unused --
  `cas-storage/Cargo.toml:18`).

Unit tests drive every walker against stores built with
`crash_fixtures` constructors (reachable in-crate as
`crate::cas::crash_fixtures::*`; they stay `#[cfg(test)]`).

## Component 4: passes and the report

Pure functions over walker outputs where possible; one record walk
powers both the recount diff and the dangling sweep.

1. **Recount**: expected (holder walk) vs actual (record walk) per
   block. Over-count INFO; under-count CRITICAL; holder references to
   a block with no record CRITICAL.
2. **Disk sweep findings**: file with no record = orphan INFO; file
   whose id has a record at a different depth = off-depth INFO; foreign
   WARN; file size != record size (for the record's own depth) =
   CRITICAL.
3. **Dangling sweep**: for each record, one stat of
   `block_disk_path(id, depth, root)` (`block.rs:139`). Missing file:
   cross-reference the disk walk's orphan set for the same id at
   another depth -> adoption candidate (repairable, INFO once
   repaired); otherwise CRITICAL, with a targeted holder re-walk to
   enumerate the blast radius, and the report names the dedup-poisoning
   propagation. A record already flagged degraded reports as INFO
   (known-damaged, awaiting heal), not a fresh CRITICAL.
4. **Corruption scrub** (`--scrub` only): read + re-hash every disk
   file against its own name with the store's hasher
   (`SharedBlockStore::hasher`, header-derived); mismatch CRITICAL.
   The wrong-width impossibility is structural
   (`BlockId::from_slice`).
5. **Multipart report**: `iter_all` over `_MULTIPART_PARTS`, group by
   (bucket, key, upload_id); report part count and total bytes per
   upload, INFO. DEVIATION from the ADR text: the part record carries
   no timestamp (`multipart.rs:8-17`), so per-upload AGE cannot be
   reported until ADR 0003 adds upload records. Report count/bytes
   only; fold this into the ADR when flipping status (component 7).
   No reaping -- report-only until ADR 0003, per the ADR.
6. **Bucket integrity**: namespace trees from `list_trees`, minus
   reserved, minus `_BUCKETS` rows (`list_buckets`,
   `meta_store.rs:253-263`) = half-deleted buckets, WARN. Their object
   records were already counted by the holder walk (hard rule 1), so
   the recount stays truthful either way.

**Report**: findings sorted CRITICAL > WARN > INFO; text render and
`--json` (serde_json). The JSON schema is part of the tool's contract:
top-level `{version, store, passes_run, findings[], summary{counts by
severity}, exit_code}`; field names decided here, in review of this
plan, not ad hoc in code.

## Component 5: repair actions

One action type per finding class, `apply(&store)`, all offline under
the exclusive open; no stripes needed (single-threaded, daemon
excluded by the fjall LOCK), but all record mutations still go through
transactions.

- **SetRc** (recount diffs, both directions -- ADR decision).
- **DeleteOrphan / DeleteOffDepth** (no record references them).
- **AdoptOrphan**: re-hash the orphan file (hard rule 3); on match,
  rewrite the record's depth to the orphan's depth (via
  `Block::from_parts`, preserving rc), clear nothing else; on
  mismatch, the file is corrupt residue -> quarantine instead.
- **QuarantineCorrupt / QuarantineForeign**: rename into
  `blocks/.quarantine/` (create + fsync dir on first use); corrupt
  blocks additionally get their record marked degraded (the ADR's
  chosen damaged-block mechanism) so future PUTs heal instead of
  dedup-hitting a poisoned record.
- **MarkDegraded**: un-adoptable dangling records get flags |=
  degraded. Idempotent by definition.
- **ResumeBucketTeardown**: for each half-deleted bucket, delete its
  objects via the striped delete primitive (`delete_object` /
  `take_object` path) and then `drop_bucket`. A non-UTF-8 object key
  must NOT panic (the daemon path expects UTF-8,
  `delete_path.rs:139`); fsck reports it WARN and skips that key.
- Post-repair recount (hard rule 2).

Tests: idempotency (apply twice, same end state); kill-mid-repair
(apply a strict subset, re-run fsck, assert clean); extend
`crash_fixtures` with the remaining constructors the ADR names:
inflated/deflated rc (`Block::from_parts`), bit-flipped block file,
half-deleted bucket (remove the `_BUCKETS` row by hand), stale part
records.

## Component 6: the qss-storage-fsck binary

REALIZATION DECISION (reversible, recorded here): the ADR's
"store-level binary homed in cas-storage" lands as a NEW thin
workspace member crate `qss-storage-fsck` (bin-only) depending on
cas-storage -- the walkers/passes/repairs all live in cas-storage's
scrub module per the ADR; making the bin a separate crate keeps clap
out of the library's dependency tree. If the owner prefers a literal
`[[bin]]` inside cas-storage, moving it later is mechanical.

- Crate: `qss-storage-fsck` added to workspace members; deps:
  cas-storage, clap (workspace), anyhow, serde_json, tokio (the
  teardown-resume repair reuses the async striped delete; the bin is
  `#[tokio::main]` like `s3cas check`).
- CLI: `--config`, `--meta-root`, `--fs-root`, `--metadata-db`,
  `--durability`, `--inline-metadata-size` resolved through the
  relocated `StoreOptions` (component 2); flags `--scrub`, `--repair`,
  `--json`.
- **Refuse to create** (the check.rs trap): before constructing
  anything, `classify_db_dir` on BOTH `<meta_root>/db` and
  `<meta_root>/blocks/db` must return `Open`, else exit 3 with "no
  store at <path>" -- the `inspect.rs:61-64` pattern. `check.rs` and
  `retrieve.rs` silently create stores on a mistyped path today; fsck
  must not (and fixing those two the same way is a flagged follow-up,
  not this plan).
- Open via `CasFS::single_namespace` + `SharedBlockStore` handles the
  same way the daemon does; opening runs the `.tmp` purge and st_dev
  check for free (`AtomicBlockWriter::open`, `block_disk.rs:161-204`).
- Exit codes: 0 clean/INFO-only, 1 WARN, 2 CRITICAL, 3 could-not-run
  (std::process::exit after flushing the report).
- Release workflow: add the binary to `release.yaml`'s build/upload
  matrix next to s3cas.
- Tests: exit-code integration tests in the bin crate; residue planted
  through public surfaces (`block_disk_path` is pub; record planting
  via `MetaStore` transactions) since `crash_fixtures` is not visible
  outside cas-storage.

## Component 7: docs, deviations, acceptance

- `docs/fsck.md`: operator page -- passes, severities, exit codes,
  JSON schema, quarantine location, the degraded-flag lifecycle, the
  "report first, repair second, recount third" workflow. Cross-link
  from `docs/refcount.md` (fsck is the reconciliation half its
  contract promises) and pair with verify-on-read docs.
- Fold the accumulated deviations into ADR 0005: the bin-as-sibling-
  crate realization; the multipart age gap (no timestamp in part
  records until ADR 0003); holder enumeration via `list_trees` rather
  than `_BUCKETS`; anything else accumulated en route.
- THEN flip ADR 0005 to Accepted as a standalone one-line commit
  (house rule: the status flip is its own commit after impl + docs are
  green, so bisect lands cleanly on the acceptance moment).

## Sequencing and risk

Order: 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7. Components 1 and 2 are
independent of each other but both feed 3+; keep them serial for
one-worktree gate discipline.

Riskiest first: component 1 touches the write path's dedup decision --
the degraded branch must stay inside the existing tx + stripe with no
new awaits (hard rule 5); the race arm is the proof. Component 5's
SetRc is the only action that can lower rc; it exists only downstream
of the closed-holder-set refusal (hard rule 1) and upstream of the
mandatory re-recount (hard rule 2).
