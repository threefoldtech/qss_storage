# ADR 0003 Implementation Plan: multipart lifecycle + stale-upload GC

Executes `docs/adr/0003-multipart-lifecycle-and-gc.md`
(decision-complete 2026-07-31). Seven components, one gate-green commit
each, in order. Verified against HEAD a5ecd6b; the file:line facts
below come from the revision discovery.

## Hard rules (apply to every component)

1. **The upload record is the only linearization point.**
   `Transaction::take_upload` (atomic read+remove, `take_object`'s
   template at `meta_store.rs:585-600`) is the single claim primitive
   for complete and abort. Nothing else serializes them; no per-upload
   lock exists.
2. **Abort ordering is load-bearing**: remove a part record BEFORE
   releasing its blocks, per part. A crash between the two leaves an
   over-count (INFO, recount collects). The reverse order fabricates a
   loss-shaped under-count fsck would repair into a real leak. State
   this in a comment at the abort loop.
3. **Every rc mutation stays inside ADR 0006's machinery**: the striped
   RMW via `decrement_block_rc` under an owned stripe guard inside
   `spawn_blocking`. `release_blocks` is a factoring of
   `delete_object`'s existing loop (`delete_path.rs:87-122`), never a
   reimplementation. fjall stays the leaf lock; no await inside a tx.
4. **Value-driven enumeration only** for reaping and listing: decode
   `MultiPart` values; never parse legacy dash keys. An old-format
   record is reaped as an orphan (no upload record can exist for it) --
   that IS the migration; there is no store-header bump.
5. Plain ASCII. Gates per commit: `make fmt` (run LAST, then `git add`
   as its own command), `cargo clippy --workspace --all-targets -- -D
   warnings`, `cargo test --workspace`. One commit per component,
   subject naming the component and the ADR. gpg-timeout rule: retry
   once after 30s, else leave staged and report.

## Component 1: uploads tree, record codec, take_upload

- `UPLOADS_TREE = "_UPLOADS"` pub const next to `MULTIPART_PARTS_TREE`
  (`meta_store.rs:38`); opened in `SharedBlockStore::new` beside the
  parts tree (`shared_block_store.rs:101-103`), new field + accessor
  (`uploads_tree()`, an ext handle -- iteration is needed).
- `UploadRecord` v1 (new `cas-storage/src/cas/uploads.rs` or inside
  `multipart.rs`): wire `created_at i64 | bucket_len u64 | bucket |
  key_len u64 | key | upload_id_len u64 | upload_id`, encode/decode via
  the `codec::Reader` house pattern with `bucket_meta.rs` as template
  (golden vectors, truncation/trailing/overflow/utf8 malformed tests).
  `created_at = chrono::Utc::now().timestamp()`.
- Upload KEY: `bucket_len u64 | bucket | key_len u64 | key | upload_id`
  bytes (unambiguous; point-get reconstructible; per-bucket scans
  filter in memory, order irrelevant -- listings sort in memory).
- `Transaction::take_upload(key) -> Result<Option<UploadRecord>>` on
  the shared-DB tx, mirroring `take_object`; plus a plain
  `upload_exists` point read for `upload_part`'s check.
- `CasFS` surface: `create_upload(bucket, key, upload_id)`,
  `get_upload`, `claim_upload` (the take_upload wrapper). No callers
  yet -- s3cas wires in component 4. Unit tests incl. the claim race
  (two claims, one winner) exercised via two sequential txs.

## Component 2: part re-key + prefix scan

- Binary part-key codec beside the record (`multipart.rs`):
  `bucket_len u64 | bucket | key_len u64 | key | upload_id_len u64 |
  upload_id | part_number u64 BE`; `part_key(...)` in `fs.rs:78-80` is
  replaced by it (same callers: insert/get/remove_multipart_part,
  `fs.rs:298-363`). `part_prefix(bucket, key, upload_id)` yields the
  shared prefix.
- Per-upload enumeration: `MultiPartTree` today wraps `BaseMetaTree`
  (no iteration, `multipart.rs:162-195`); scrub goes around it via
  `get_tree_ext(MULTIPART_PARTS_TREE)` (`holders.rs:194-201`). Give
  `MultiPartTree` the ext handle instead and add
  `parts_of(prefix) -> impl Iterator<...>` implemented as
  `iter_kv(start_after: just-below-prefix)` + stop-at-first-nonmatch;
  scrub may keep its own path (do not refactor scrub here).
- Tests: round-trip, prefix-scan yields exactly one upload's parts in
  part_number order (u64 BE sorts), dash-named legacy record is NOT
  matched by any prefix scan (plant via raw tree insert).

## Component 3: release_blocks

- Factor `delete_object`'s striped decrement loop
  (`delete_path.rs:87-122`) into
  `pub(crate) async fn release_blocks(fs-or-shared handles, blocks:
  &[BlockId], metrics)` -- per-occurrence, stripe guard moved into the
  blocking closure with `decrement_one_block` (`delete_path.rs:32-60`),
  errors logged and the loop never aborted. `delete_object` becomes its
  first caller; behavior identical (existing delete tests must pass
  untouched).
- Race-test arm: release_blocks vs concurrent same-content PUT --
  the PUT's striped bump serializes against the release's striped
  decrement; assert exact final rc both interleavings.

## Component 4: s3cas handlers + enforcement

- `abort_multipart_upload` in `impl S3 for S3FS` (`s3fs.rs`): claim via
  `claim_upload`; `None` -> `s3_error!(NoSuchUpload, ...)` (decided);
  else prefix-scan parts, per part remove-record-then-release_blocks
  (hard rule 2); return empty `AbortMultipartUploadOutput`.
- `upload_part` (`s3fs.rs:725-799`): existence check at entry
  (`get_upload`), `NoSuchUpload` if absent -- BEFORE `store_object`
  streams blocks.
- `complete_multipart_upload` (`s3fs.rs:104-195`): reject an empty
  parts list up front (`InvalidRequest`-class error: "You must specify
  at least one part"); claim the upload record (replacing no-check);
  claim failure -> `NoSuchUpload`. The claim happens BEFORE object
  creation; a crash after claim + before object-meta leaves orphan
  parts (GC's problem, leak-bounded -- comment it).
- `create_multipart_upload` (`s3fs.rs:232-252`): write the upload
  record after the bucket check.
- `list_parts`: prefix-scan, decode values, in-memory sort by
  part_number, honor `part_number_marker`/`max_parts`/`is_truncated`/
  `next_part_number_marker`. `NoSuchUpload` if no upload record.
- `list_multipart_uploads`: iterate `_UPLOADS`, filter bucket (+
  `prefix` if given), in-memory sort by (key, upload_id), honor
  `key_marker`/`upload_id_marker`/`max_uploads`; `delimiter` ignored
  (deferred, ADR open question); `initiated` from created_at.
- `MetricFs` (`s3cas/src/metrics.rs:269-411`) gains the three wrappers;
  `S3_API_METHODS` (`metrics.rs:11-28`) gains the three names.
- Integration tests (the `it_s3` pattern): abort releases refcounts and
  files; double-abort NoSuchUpload; complete-vs-abort race (spawn
  both, exactly one wins); upload_part to unknown id; empty complete;
  listing order + pagination; part-after-abort lands as orphan (blocks
  held by part record only).

## Component 5: GC task + config + metrics

- Sweep function in cas-storage (`cas/gc.rs` or beside uploads):
  `pub async fn sweep_stale_uploads(fs: &CasFS, ttl: Duration) ->
  SweepStats`: (a) iterate `_UPLOADS`, claim-and-abort records older
  than ttl through the same abort internals as the handler (share the
  code -- the handler's body factors into a `CasFS::abort_upload`
  callable by both); (b) iterate `_MULTIPART_PARTS` values, collect
  records whose upload record does not exist (point read), remove
  record then release_blocks (hard rules 2, 4). Returns counts.
- `CasFS` gains `#[derive(Clone)]` (all fields cheap-clone:
  `MetaStore` is Clone, `Arc<SharedBlockStore>`, `SharedMetrics`,
  bool). One-line doc note: a clone shares the SharedBlockStore, so
  the one-store-one-instance rule (ADR 0006) is preserved.
- Daemon task (`s3cas/src/main.rs`): clone `casfs` before it moves into
  `S3FS::new` (`main.rs:302`); spawn before the accept loop; loop
  `tokio::select!` on `interval.tick()` (period `max(ttl/20, 1h)`) vs
  a shutdown `watch`/`Notify` wired to the existing ctrl_c break
  (`main.rs:404-406`); skip entirely when ttl == 0 (disabled).
- Config (`cas-storage/src/config.rs` house pattern, `config.rs:18-32`):
  `MultipartConfig { stale_ttl_days: Option<u64> }` with
  deny_unknown_fields; `pub multipart: Option<MultipartConfig>` on
  `QssStorageConfig`; `DEFAULT_MULTIPART_STALE_TTL_DAYS: u64 = 7`;
  update the three config-test fixtures (`FULL` config.rs:370-396,
  example-file test :434-457, absent-keys :472-489) AND
  `qss_storage.toml.example`; s3cas CLI flag `--multipart-stale-ttl-days`
  merged flag-over-file-over-default in `resolve_server`
  (`main.rs:108-149`).
- Metrics (`s3cas/src/metrics.rs` pattern): `uploads_reaped`,
  `orphan_parts_reaped` IntCounters, incremented by the daemon task
  from SweepStats.
- Tests: sweep reaps an aged upload (plant with a backdated
  created_at via a crate-internal constructor) and not a fresh one;
  orphan sweep reaps a planted stale part (fixture exists:
  `plant_stale_part`, `crash_fixtures.rs:160-178`) and a legacy
  dash-keyed record; ttl=0 never spawns.

## Component 6: fsck upgrade

- Multipart pass (`scrub/passes.rs:381-441`): read `_UPLOADS`; per
  upload report AGE (now - created_at) alongside parts/bytes; the
  "age arrives with ADR 0003" deviation text dies. Uploads with no
  parts are reported too (age only).
- New `FindingClass::OrphanPart` (INFO): part record whose upload
  record does not exist. Emitted by the multipart pass (it already
  iterates parts).
- Repair action: remove the orphan part record (nothing else) -- the
  engine's closing recount then lowers freed blocks' expected counts
  and existing SetRc machinery reclaims. Gate on `Pass::Recount` like
  every rc-consequential action.
- Fixtures: `plant_upload_record` (arbitrary created_at);
  orphan-part-vs-live-part classification test; end-to-end: plant
  orphan part -> report INFO -> --repair -> post-report clean and
  blocks freed.
- docs/fsck.md: the multipart section updates (age, orphan_part, the
  GC as primary reaper).

## Component 7: docs, deviations, acceptance

- Operator docs: multipart lifecycle section (where it fits --
  docs/multipart.md or an existing page): TTL semantics, the claim
  rule, orphan parts, what clients see (NoSuchUpload cases).
- Fold accumulated deviations into ADR 0003 marked "(as built)".
- Update `qss_storage.toml.example` comments if not done in C5.
- THEN flip ADR 0003 to Accepted as a standalone one-line commit
  naming the landing series (house rule).

## Sequencing and risk

Order: 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7. Components 1-3 are cas-storage
foundations (independent of each other in principle; serial for
one-worktree gate discipline). Riskiest: component 4's claim wiring
(the linearization rule) and component 3's factoring (must not change
delete_object behavior); component 5's clock-based test needs a
backdatable constructor, not sleeps.
