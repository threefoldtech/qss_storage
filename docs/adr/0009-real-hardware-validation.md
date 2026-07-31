# Real-Hardware Validation: aws-cli, valkey-cli, NVMe

**Status**: Proposed
**Date**: 2026-08-01

---

## Context

Everything this codebase guarantees has been verified by in-process
tests: unit suites, the it_s3 integration tests (s3s harness, loopback,
tempdirs), race storms, crash fixtures, benches. Nothing has ever been
exercised by a real client over a real wire against a real disk:

- The S3 surface has never seen aws-cli: real SigV4 signing, chunked
  transfer encoding, retry behavior, multipart part sizing chosen by
  the client, pagination as clients actually drive it, error-code
  handling by software we do not control.
- The RESP surface has never seen a real valkey/redis client:
  pipelining, RESP2/RESP3 negotiation, inline commands, client-side
  timeouts, large-value framing.
- The store has never run on a real filesystem on real flash: xfs
  fsync/fdatasync semantics, rename durability, directory-entry
  behavior at real fanout sizes, st_dev on a real mount, ENOSPC,
  and the actual latency profile the durability chain buys.

The gap matters because the strongest claims -- file-first durability
(ADR 0006), loss-never refcounts, crash residue always leak-shaped
(ADR 0005), the multipart claim rule (ADR 0003) -- are precisely the
claims whose failure modes live below the layers the in-process
harnesses fake.

Hardware designated by the owner: `nvme1n1`, xfs, mounted at `/s3`,
owned by the operating user. The campaign never provisions hardware:
mkfs and mount are the owner's, done once, outside the scripts.

Related: every implemented ADR (0002-0008 as they land); docs/fsck.md,
docs/multipart.md (the behaviors under test); `Makefile` (gates).

---

## Decision

Proposed, pending review: a **scripted, repeatable validation campaign
in-repo** -- `tests/real/` -- driven by one entrypoint with numbered
phases, each phase with explicit pass criteria, runnable end-to-end or
per phase. Not a checklist, not CI: a campaign an operator runs on the
designated hardware and reads a verdict from.

### Ground rules

1. **Safety rail**: the driver refuses to run unless the store root is
   exactly `/s3`, it is an xfs mount (checked via `stat -f`), it is
   writable by the invoking user, and a sentinel file
   `/s3/.qss-realtest` exists (created once by the owner, proof of
   intent). The scripts never mkfs, never mount, never sudo, and never
   touch a path outside `/s3` (store) and the results dir.
2. **Results** live outside the store: `target/realtest/<timestamp>/`
   -- per-phase logs, timing, the daemon's own logs, fsck reports
   (text + JSON), and a final `verdict.md` summarizing pass/fail per
   phase. The store must never contain campaign bookkeeping.
3. **Config**: a campaign-owned `tests/real/qss_storage-realtest.toml`
   (store root `/s3`, fsync durability by default, short multipart TTL
   for the GC phase); release binaries (`cargo build --release`)
   only -- the campaign measures the artifact we ship.
4. **Tools**: aws-cli v2 (`aws --endpoint-url`, path-style), valkey-cli
   (RESP), plus coreutils. Preflight verifies versions and refuses on
   absence rather than degrading silently.
5. Every phase is idempotent to re-run: each starts by asserting or
   creating its own namespace/bucket and ends by reporting, not by
   cleaning (later phases and fsck want the accumulated state; the
   final phase measures teardown).

### Phases

0. **Preflight**: mount/fs/ownership/sentinel checks; tool versions;
   release build; store empty or explicitly resumed (`--resume`).
1. **S3 functional (aws-cli)**: bucket lifecycle; object CRUD at 1 B,
   1 KiB (inline boundary both sides), 1 MiB (block boundary), 100 MiB,
   1 GiB; ETag verified against locally computed MD5 for single-part;
   round-trip byte-compare on every GET (`cmp`); listings with >1000
   keys (pagination), prefixes, delete-objects batches; error paths
   (NoSuchBucket, NoSuchKey, double-create).
2. **Multipart lifecycle (aws-cli + s3api)**: `aws s3 cp` at sizes
   forcing client-chosen multipart (2 GiB+); manual s3api
   create/upload-part/complete with out-of-order parts; multipart ETag
   verified against the MD5-of-MD5s convention; abort mid-upload and
   verify listings show nothing; `list-multipart-uploads` /
   `list-parts` against concurrent uploads; the retryable-failed-
   complete behavior (name a missing part, then correct it).
3. **GC observation**: with the short-TTL campaign config, create
   uploads, abandon them, wait out the TTL + sweep, verify reaping via
   `list-multipart-uploads` (empty), metrics counters
   (`s3_multipart_uploads_reaped`), and disk usage returning to
   baseline.
4. **RESP functional (valkey-cli)**: the full documented respd command
   surface (enumerated during implementation from respd's docs/code --
   a campaign discovery step, pinned in the phase script): namespaces,
   SET/GET round-trips at inline sizes and the maximum accepted value,
   binary-safe payloads (embedded NUL, high bytes), wrong-type/absent-
   key errors, pipelined batches (`valkey-cli --pipe`), concurrent
   clients.
5. **Concurrency and stress**: parallel aws-cli workers (`xargs -P`,
   distinct keys) sustaining mixed put/get/delete for a fixed wall
   time; overlapping same-key overwrite storms; target: zero client
   errors, metrics sane, memory/fd stable (sampled via /proc), then a
   metadata-only fsck recount: findings must be leak-class only.
6. **Crash and recovery**: `kill -9` the daemon mid-multipart-storm and
   mid-PUT-storm (scripted signal at randomized delay); restart;
   verify every operation the client saw succeed is readable and
   byte-identical; fsck report must contain only the documented
   residue classes (orphans, over-counts, stale parts -- never
   under-counts, never corruption); `--repair` converges (second run
   clean); repeat the cycle several times.
7. **Durability matrix**: rerun phase 6 once at `durability = buffer`
   and document the observed difference honestly (buffer accepts
   power-loss dangling records by contract -- a kill -9 is NOT power
   loss, so the difference should be residue volume, not loss;
   any client-visible loss at either level fails the campaign).
8. **Full scrub**: `qss-storage-fsck --scrub` over the fully populated
   store (disk-bound, whole-store rehash): zero corruption findings;
   record the throughput (the ADR 0005 sizing estimate meets reality
   here).
9. **Teardown measurement**: delete everything through the clients
   (S3 deletes, respd deletes, bucket removal); final fsck: the store
   should reconcile to empty (or enumerate exactly what remains and
   why); record reclaimed-space curve.

### Pass criteria (campaign-level)

Zero client-visible errors in functional phases; every byte-compare
identical; post-crash fsck findings restricted to documented leak
classes; scrub clean; repair convergent; no daemon panics anywhere in
the logs; metrics endpoints alive throughout. Anything else is a
finding, filed with its phase log, and the verdict says FAIL.

---

## Alternatives Considered

### Manual checklist instead of scripts
- **The idea**: a documented procedure the owner walks through.
- **Optimizes for**: zero script maintenance.
- **Sharpest tradeoff**: not repeatable, not comparable across runs,
  and the interesting failures (races, crash windows) need scripted
  timing anyway.
- **Bets on**: running this once. The point is to rerun it every
  release.

### External conformance suites (ceph s3-tests, MinIO mint)
- **The idea**: run an established S3 conformance suite instead of
  hand-built phases.
- **Optimizes for**: breadth of S3-surface coverage for free.
- **Sharpest tradeoff**: huge unimplemented-feature noise (versioning,
  ACLs, policies) drowning the signal; no RESP coverage; no crash or
  durability phases -- the part this campaign uniquely adds.
- **Bets on**: conformance breadth mattering more than depth on our
  claims. Not rejected -- deferred as an optional phase 10 (a curated
  subset), review ask 3.

### CI-cloud execution
- **The idea**: run the campaign in CI on cloud instances.
- **Sharpest tradeoff**: no real NVMe, no stable hardware identity,
  and cloud-instance fsync semantics are exactly the thing being
  faked. The campaign's value is the real disk.
- **Bets on**: nothing -- it simply does not measure what this
  measures. CI keeps the in-process suites.

---

## Consequences

### Positive
- The headline guarantees get exercised where their failure modes
  actually live; every release can be re-validated on hardware with
  one command.
- The campaign artifacts (fsck JSON, timing, throughput) become the
  performance baseline the benches cannot provide.

### Negative
- `tests/real/` is shell-script surface to maintain alongside the Rust
  suites; client-tool version drift (aws-cli behavior changes) can
  break phases for non-storage reasons.
- The campaign consumes the designated disk and hours of wall time;
  it is explicitly not CI.

### Risks
- Destructive-by-design phases (kill -9, ENOSPC if added later) on a
  machine that has other duties: mitigated by the /s3-only rail and
  the sentinel; the scripts never escalate privileges.
- A flaky client tool (aws-cli retry masking a real error): mitigated
  by capturing daemon logs per phase and failing on any daemon-side
  error regardless of client verdict.

---

## What an Expert Would Ask

**Q: kill -9 is not power loss -- what do phases 6/7 actually prove?**
A: Process-crash atomicity: the ordering guarantees that survive the
page cache (rename-before-record, claim atomicity, residue classes).
Honest scope: fsync-vs-power-loss needs a power-cut rig or dm-flakey
style fault injection -- out of scope here, recorded as the known gap.
The campaign proves the crash-consistency layer above it; dm-flakey is
the natural phase 11 if the owner wants it later (open question).

**Q: How long does a full run take?**
A: Dominated by dataset writes and the scrub: at NVMe speeds, the
functional phases are minutes; stress and soak are configured wall
time (default 30 min); the scrub reads the whole store once. Budget
2-4 hours default, tunable via a SCALE knob (dataset multiplier) in
the campaign config.

**Q: What stops /s3 residue from one run poisoning the next?**
A: The preflight refuses a non-empty store unless `--resume` is
explicit; a `--fresh` flag wipes the store's own directories (never
the mount) after fsck confirms nothing unexpected lives there.

**Q: Why is the RESP phase thinner than S3?**
A: respd's surface is thinner (inline-only storage, no multipart, no
GC). The phase covers what exists; if respd grows block-backed values,
the phase grows with it -- the enumeration step at the start of the
phase script is the tripwire.

---

## Implementation Plan

### Decisions you will probably want to tweak
- **Layout**: `tests/real/` with `run.sh` + `phases/NN-name.sh` +
  `lib.sh` (asserts, logging, verdict). Alternative: a Rust binary
  driving everything. Shell chosen: the campaign IS the real clients;
  wrapping them in Rust adds nothing but compile time. Cost to
  change: moderate.
- **Dataset scale defaults** (1 GiB single, 2 GiB multipart, >1000-key
  listings, 30-min stress): sized for a first run; the SCALE knob
  multiplies. Review ask 1.
- **Verdict format**: `verdict.md` + exit code (0 pass, 1 findings,
  2 fail). Scripting contract once shipped.

### Known unknowns and how the plan absorbs them
- respd's exact command surface: enumerated during implementation;
  the phase script pins it and fails if the surface grows unnoticed.
- aws-cli multipart chunk sizing defaults change across versions: the
  phase logs the tool version and pins part size explicitly where the
  test depends on it.

### The mechanical work
`tests/real/` scaffolding (driver, lib, preflight); the campaign
config; phases 1-9 as scripts with per-phase pass criteria; a
`make realtest` target; docs page `docs/realtest.md` (how to prepare
the disk, run, read the verdict); .gitignore for target/realtest.

Review asks:
1. Dataset scale defaults (1 GiB / 2 GiB / 30-min stress) -- right
   first-run sizing for the nvme1n1 disk?
2. Phase 7's durability matrix at buffer level -- include in the
   default run or flag-gated (it doubles the crash-phase time)?
3. Optional phase 10: curated ceph/s3-tests subset -- include now,
   later, or never?
4. `--fresh` wipe semantics (fsck-verified store-dir wipe, never the
   mount) -- acceptable?

---

## Open Questions

**Architecture-changers**
- [ ] Power-loss-grade fault injection (dm-flakey / a power-cut rig)
      as a future phase 11: is there appetite, or is process-crash
      grade the accepted ceiling for this campaign?

**Behavior definers**
- [ ] Should the campaign also run against a store on the OS disk
      (worst case: shared, slower) to catch same-device assumptions,
      or is /s3-only the contract?
