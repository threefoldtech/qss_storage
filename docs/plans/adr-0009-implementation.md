# ADR 0009 Implementation Plan: the real-hardware validation campaign

**Status**: IN PROGRESS. Executes
`docs/adr/0009-real-hardware-validation.md` (Accepted 2026-07-31, all five
review asks answered, both open questions closed). Verified against
`development` at `ec1c2f2` -- ADR 0008 landed, so refcounts are exact
through the whole object lifecycle and this campaign asserts the post-0008
semantics, not the pre-0008 allowances.

The ADR is the authority on *why* and on *what each phase proves*. This is
the component-level *what*, written to be executed in a fresh session with
no other context. It is a component spec, not a schedule: every component
below stands alone and names its own acceptance.

**What is being built here is the HARNESS.** The campaign itself runs later,
on the owner's 3.6 TB NVMe at `/s3`, which this session does not have and
never touches. Everything here is written to be verifiable against a
tempdir store first and correct on the real disk second.

## Decisions already made -- do not reopen (rationale in the ADR)

- **Shell, not Rust.** The campaign IS the real clients (aws-cli,
  valkey-cli); wrapping them in Rust adds compile time and nothing else.
- **Layout**: `tests/real/` with `run.sh` (single entrypoint, numbered
  phases), `phases/NN-name.sh`, `lib.sh`, a preflight phase, and a campaign
  config file.
- **Scale defaults are max for a 3.6 TB disk**: 4 GiB single-part PUT (just
  under the 5 GiB single-PUT ceiling), 16 GiB client-driven multipart,
  >1000-key listings, 60-minute stress wall clock. `SCALE` **divides**
  these, for smoke runs.
- **Phase 7 (buffer-level durability matrix) is in the DEFAULT run.**
- **Crash grade is `kill -9` and restart, ONLY.** No dm-flakey, no
  power-off, no broken-disk simulation anywhere in this campaign.
- **`--fresh`** wipes the store's own directory only, after fsck confirms it
  is a qss store (or the directory is empty). It never touches the mount.
  Preflight refuses a non-empty store without `--resume` or `--fresh`.
- **Phase 10 (`--tb`)** fills ~3 TiB capped at 85% of the filesystem, from a
  seeded per-key deterministic generator -- there is no second copy of the
  data anywhere.
- **Verdict contract**: `verdict.md` plus exit code 0 pass / 1 findings /
  2 fail. Daemon logs captured per phase; any daemon-side error fails the
  phase regardless of the client verdict.
- **A curated ceph/s3-tests subset is deferred.** Do not scaffold it.

## Hard rules (apply to every component)

1. **Nothing outside the store root and the results dir is ever written.**
   The store root lives under the mount; the results dir lives under
   `target/`. No campaign bookkeeping inside the store, ever -- the store's
   contents are the thing under test and fsck reports anything else as a
   foreign file.
2. **The safety rail is refusal, not degradation.** A missing tool, a
   non-xfs mount, an absent sentinel: refuse and say which check failed.
   The one escape hatch is an environment variable named
   `QSSRT_UNSAFE_ALLOW_ANY_PATH`, and a run that uses it is stamped
   NOT CAMPAIGN GRADE in its own verdict.
3. **Every assertion produces a line in the phase's `checks.tsv`**, with
   status, description and evidence. The verdict is a fold over those
   files, never over ad-hoc script exit codes.
4. **Phases are idempotent to re-run** and never clean up after themselves:
   later phases and fsck want the accumulated state, and phase 9 measures
   the teardown.
5. **Release binaries only** (`cargo build --release`): the campaign
   measures the artifact we ship. The binary directory is a config knob so
   the harness can be smoke-tested, and the resolved path is recorded in
   every run.
6. **Plain ASCII, `bash`, `set -uo pipefail`** (never bare `set -e`: a
   failing check must be recorded, not abort the phase). `bash -n` on every
   script before every commit; `shellcheck` when it is installed.

---

## Component 0: reconnaissance -- what the real clients actually do

Ran first, against the release binaries on a tempdir store, because the
pass criteria are worthless if they are guesses. aws-cli 2.36.13,
valkey-cli 9.1.1, bash 5.3. Every fact below is measured, not read:

| Probe | Result |
| --- | --- |
| `s3api put-object` with 2.36 defaults (CRC32 request checksums, `aws-chunked`) | works; ETag is the body MD5 |
| `s3 cp` of 24 MiB (client-driven multipart at the 8 MiB threshold) | works |
| `get-object` round trip, `cmp` | byte-identical |
| `get-object` ETag | present |
| `head-object` ETag | **absent** -- `head_object` does not set `e_tag` (`s3cas/src/s3fs.rs:616`) |
| `list-objects-v2` paginator over 1200 keys | **1000 keys, silently** -- the V2 output never sets `IsTruncated`, and botocore's V2 paginator stops on it |
| `s3 ls --recursive` over 1200 keys | **1000 keys, silently** -- same cause |
| manual V2 loop on `NextContinuationToken` | 1200 keys, two pages: the server contract is fine, the client-visible one is not |
| `list-objects` (V1) paginator | 1200 keys -- botocore falls back to `Contents[-1].Key` when `NextMarker` is null |
| `list-objects-v2` on a bucket that does not exist | **200 and an empty listing**, and the bucket tree is created as a side effect: a `put-object` to that name then succeeds, while `list-buckets` never shows it |
| `head-bucket` / `put-object` / `get-object` on a genuinely untouched name | correct `NoSuchBucket` / 404 |
| `delete-object` of a missing key | `NoSuchKey` |
| `create-bucket` twice | `BucketAlreadyExists` |
| metrics endpoint | 31 `s3_*` series, bound to `::1` when `--metric-host` is left at `localhost` |
| respd dispatch surface | exactly 20 commands (below) |

Four of these are findings the campaign exists to produce, and the phases
below assert the correct behaviour rather than the observed one -- a
harness written to pass against today's bugs is a harness that certifies
them. Expect phases 1 and 9 to report them on the first real run.

The metrics binding is not a finding, it is a configuration lesson: the
campaign config sets `127.0.0.1` explicitly for every endpoint so the
scrape address is never ambiguous.

**respd's surface, pinned** (from `respd/src/cmd.rs`'s dispatch): AUTH,
CHECK, DBSIZE, DEL, EXISTS, FLUSH, GET, KEYTIME, LENGTH, MGET, NSINFO,
NSLIST, NSNEW, NSSET, PING, RSCAN, SCAN, SELECT, SET, TIME. The ADR wants
this enumeration to be a tripwire, so phase 4 re-derives the list from the
source at run time and fails if it differs from the pinned copy.

**The store layout the harness addresses** (`docs/fsck.md`):
`<root>/db/` namespace DB, `<root>/blocks/db/` shared blocks DB,
`<root>/blocks/<xx>/<full-hex>` block data files (one fanout level),
`<root>/blocks/.tmp/`, `<root>/blocks/.quarantine/`. Counting live block
files means walking `blocks/` at depth 2 with `db`, `.tmp` and
`.quarantine` pruned.

**The inline boundary is not a constant.** `inline_metadata_size` defaults
to 1, and the payload that actually fits is
`inline_metadata_size - (OBJECT_HEADER_SIZE + 8)`, so the campaign config
sets it explicitly and phase 1 *measures* the boundary by watching the
block-file count instead of asserting an arithmetic guess.

## Component 1: `lib.sh` -- asserts, logging, verdict primitives

`tests/real/lib.sh` is the only file phases source. It sources
`tests/real/lib/*.sh` and owns the vocabulary:

```
phase_begin <nn> <name>       opens the phase dir, starts the log and timer
check_pass  <desc> [evidence] records PASS
check_fail  <desc> <evidence> records FAIL    (campaign-level violation)
check_find  <desc> <evidence> records FINDING (deviation, not loss)
check_skip  <desc> <reason>   records SKIP    (not run, and why)
assert_eq <desc> <expected> <actual>
assert_ok <desc> -- <cmd...>          command must exit 0
assert_err <desc> <code-substr> -- <cmd...>   must fail, naming the S3 code
assert_same <desc> <fileA> <fileB>    byte-compare (cmp), evidence on diff
phase_end                     writes timing, returns the phase's worst status
log / logf / note             human-readable narration into the phase log
```

Statuses are exactly four: PASS, FAIL, FINDING, SKIP. They land in
`checks.tsv` as `status<TAB>description<TAB>evidence`; nothing else writes
that file. The grading fold is fixed here and nowhere else:

- any FAIL in any phase -> run exit 2;
- else any FINDING -> run exit 1;
- else 0. SKIP never changes the exit code but is always listed.

**Why FAIL and FINDING are separate**: the ADR's pass criteria are about
loss, corruption, panics and client-visible errors. A deviation that is
none of those (a slower-than-expected scrub, INFO-class residue after a
crash phase, an optional check that could not run) still belongs in the
verdict, but it is not the same statement. The split is what makes exit 1
mean something.

Acceptance: `bash -n`; a self-test phase (`tests/real/lib/selftest.sh`,
run by `run.sh --selftest`) that exercises each primitive against
`/dev/null` fixtures and asserts the grading fold, including that a phase
script which dies mid-way is recorded as a FAIL rather than silently
missing.

## Component 2: the campaign config and the daemon config

Two files, two audiences.

**`tests/real/campaign.conf`** -- shell `KEY=${KEY:-default}` lines, sourced
by `run.sh`, every value overridable from the environment. It holds the
mount and store paths, the endpoints and credentials, the results root, the
binary directory and profile, the seed, and every size:

```
QSSRT_MOUNT=/s3                    the mount the rail checks
QSSRT_STORE_ROOT=/s3/qss-realtest  the campaign's own directory under it
                                   ($STORE_ROOT/s3 = the S3 store,
                                    $STORE_ROOT/resp = respd's data dir)
QSSRT_RESULTS_ROOT=<repo>/target/realtest
QSSRT_SCALE=1                      divides every size below
QSSRT_SINGLE_PART_BYTES=4GiB       phase 1's largest single PUT
QSSRT_MULTIPART_BYTES=16GiB        phase 2's client-driven multipart
QSSRT_LIST_KEYS=1200               phase 1's pagination corpus
QSSRT_STRESS_SECONDS=3600          phase 5's wall clock
QSSRT_TB_*                         phase 10's bands
```

`QSSRT_STORE_ROOT` is a *directory under* the mount, never the mount
itself. The ADR's ground rule says "the store root is exactly `/s3`", and
its answered review ask 4 says `--fresh` "wipes the store's own directories
(never the mount)". Those two cannot both hold: a store rooted at the mount
point has no own directory to wipe. Review ask 4 is the later and more
specific decision, so the mount is `/s3`, the store root is a directory
under it, and the rail checks the mount. Recorded here rather than
silently chosen.

**`tests/real/qss_storage-realtest.toml`** -- the daemon config the campaign
runs on: `durability = "fsync"`, `metadata_db = "fjall"`,
`inline_metadata_size` set explicitly (so phase 1's inline boundary is
exercised at all), `stale_ttl_days = 1` (the shortest the daemon accepts),
endpoints on `127.0.0.1`, credentials the campaign generates. Paths are NOT
in this file -- `--fs-root` and `--meta-root` are CLI-only by design, and
the harness passes them.

**Scale arithmetic**: sizes are written as `4GiB` and parsed by one
function; `SCALE` divides and clamps to a floor per size so a `SCALE=1024`
smoke run still exercises the shape (a multipart that scales below the
5 MiB minimum part size stops being a multipart test). The floors are
listed in the config beside each size.

Acceptance: `run.sh --print-config` renders every resolved value with its
source (default / config file / environment), and the smoke run at
`SCALE=4096` produces sizes that are still structurally valid.

## Component 3: the safety rail and preflight (phase 0)

`tests/real/lib/rail.sh` plus `tests/real/phases/00-preflight.sh`.

The rail, in order, each an individual check line:

1. `QSSRT_MOUNT` is a mount point (st_dev differs from its parent, or
   `findmnt` confirms it);
2. its filesystem type is xfs (`stat -f -c %T`);
3. it is writable by the invoking user and owned by them;
4. the sentinel `$QSSRT_MOUNT/.qss-realtest` exists (proof of intent,
   created once by the owner, never by the scripts);
5. `QSSRT_STORE_ROOT` is strictly below the mount and is not the mount;
6. free space is sufficient for the selected phases (85% cap for `--tb`);
7. tool versions: aws-cli v2 present, valkey-cli present, plus `cmp`,
   `md5sum`, `openssl`, `xargs`, `stat`, `df`, `curl`. Absence is a
   refusal, not a degradation -- except that a *smoke* run may downgrade a
   missing client to SKIP for the phases that need it, and says so;
8. the release binaries exist (built by the driver unless `--no-build`);
9. the store is empty, or `--resume`, or `--fresh` was passed.

`QSSRT_UNSAFE_ALLOW_ANY_PATH=1` turns checks 1-4 into SKIP lines and puts
a NOT CAMPAIGN GRADE banner at the top of `verdict.md`. That is the only
way the harness runs against a tempdir, and it is loud enough that nobody
mistakes a smoke verdict for a campaign verdict.

`--fresh`, exactly: refuse unless `QSSRT_STORE_ROOT` is below the mount
and is not the mount; if it does not exist or is empty, do nothing; else
require it to look like this campaign's own store
(`$STORE_ROOT/s3/db` and `$STORE_ROOT/s3/blocks/db` both classify as
existing stores) and require every top-level entry to be one the campaign
created (`s3`, `resp`); then delete those children and nothing else. Any
surprise is a refusal with the path named. The mount is never an argument
to a delete, in any code path.

Acceptance: `bash -n`; unit-ish checks in the self-test that `--fresh`
refuses a directory holding a stranger's file, refuses when the store root
equals the mount, and is a no-op on an empty directory.

## Component 4: daemon control and the daemon-side error gate

`tests/real/lib/daemon.sh`.

```
s3d_start [--durability <level>]   start s3cas server on the campaign config
s3d_wait_ready                     poll the endpoint until it answers
s3d_stop                           SIGINT, then wait for graceful exit
s3d_kill9                          kill -9, wait for the pid to be gone
s3d_restart                        stop-or-kill, start, wait ready
respd_start / respd_stop           the same for respd
daemon_log_mark <phase>            record the byte offset of every daemon log
daemon_log_slice <phase>           the log written during that phase
daemon_gate <phase>                the ADR's rule, below
```

The daemon's stdout+stderr go to `$RESULTS/daemon/s3cas.log` (one file for
the whole run, sliced per phase by byte offset -- a per-phase file would
lose whatever a crash wrote between phases). `daemon_gate` scans the slice
for ` ERROR` and `panicked at` and fails the phase for anything not
declared expected by `daemon_expect <regex>` earlier in that phase. Phases
2, 3 and 6 provoke daemon-side errors on purpose (a complete naming a
missing part, an abort, a kill mid-write) and declare exactly those.

Two facts this component is written around, both from component 0:
`s3cas`'s subscriber is pinned at INFO and logs every request with its
whole input, so a three-million-object band produces on the order of a
gigabyte of log; and the log is on the OS disk, not the mount. The config
knob `QSSRT_DAEMON_LOG_FILTER` (default off, forced on for phase 10) pipes
the daemon through a line-buffered filter that keeps WARN, ERROR,
`panicked`, and the startup/GC lines -- the gate keeps working because it
only ever reads those. The unfiltered volume is recorded either way.

Acceptance: start/ready/stop/kill9/restart against a tempdir store, with
the log slice for a phase containing that phase's requests and not the
previous phase's.

## Component 5: the deterministic content generator

`tests/real/lib/gen.sh`. The terabyte phase's central constraint: there is
no second copy of the data anywhere, so content must be a pure function of
the key, streamable at NVMe speed in both directions.

```
gen_stream <key> <size>     writes <size> bytes of key-derived content
gen_md5 <key> <size>        the MD5 of that content (for ETag checks)
gen_verify <key> <size>     reads a stream on stdin, byte-compares it
```

Implementation: an AES-256-CTR keystream over `/dev/zero`, with the key and
IV derived from `sha256(seed || NUL || key)`. Explicit `-K`/`-iv` rather
than `-pass`, because password-derived keys depend on openssl's KDF and
must not: the campaign has to reproduce the same bytes years later on a
different build. Measured: deterministic across invocations, varies with
key and with seed, and a short read is a prefix of a long one (so ranged
GETs verify against the same generator). Throughput measured at ~5 GB/s on
one core -- the client, not the generator, is the bottleneck.

Acceptance: the self-test asserts all four properties (determinism,
key-variance, seed-variance, prefix) plus that `gen_md5` agrees with
`gen_stream | md5sum`.

## Component 6: fsck invocation and finding-class assertions

`tests/real/lib/fsck.sh`.

```
fsck_run <label> [--scrub] [--repair]    daemon must be stopped; captures
                                         text + JSON reports into the phase
fsck_exit                                the exit code of the last run
fsck_count <class>                       findings of a class, from the JSON
fsck_assert_clean <desc>                 zero findings at all
fsck_assert_leak_only <desc>             zero WARN, zero CRITICAL: only the
                                         documented leak classes
fsck_assert_converged <desc>             after --repair: none of the classes
                                         --repair claims to fix survive
```

The class lists are pinned from `cas-storage/src/scrub/findings.rs` and
`docs/fsck.md`, not invented:

- leak classes (INFO): `refcount_over_count`, `orphan_file`,
  `off_depth_file`, `adoptable_dangling_record`, `degraded_record`,
  `multipart_upload`, `orphan_part`;
- repair-convergent classes: `refcount_over_count`, `refcount_under_count`,
  `orphan_file`, `off_depth_file`, `adoptable_dangling_record`,
  `orphan_part`, `half_deleted_bucket`, `corrupt_block` (quarantined),
  plus `post_repair_recount_dirty` which must never appear;
- classes `--repair` is documented not to fix, and which therefore do not
  count against convergence: `missing_block_record`,
  `undecodable_block_record`, `size_mismatch`, `degraded_record`,
  `multipart_upload`, `holder_enumeration_failed`.

fsck is offline and inherits exclusivity from fjall's LOCK, so every
`fsck_run` asserts the daemon is stopped first and reports exit 3
(could-not-run) as a phase FAIL with the lock named.

JSON parsing goes through `jq` when it is present and through a pinned
`grep`/`sed` fallback on the text report's summary line otherwise -- the
text summary (`N critical, M warn, K info (T finding(s)); exit E`) is a
documented rendering, so the fallback is not a guess.

Acceptance: against a tempdir store, a clean store reports clean; a store
with a planted orphan file (a stray file in a fanout dir) reports exactly
`orphan_file` and `--repair` converges.

## Component 7: `run.sh` -- the driver

Single entrypoint. Flags:

```
--phase N (repeatable) / --from N / --list    phase selection
--tb                                          add phase 10
--fresh / --resume                            store precondition
--scale N                                     override QSSRT_SCALE
--no-build                                    skip cargo build --release
--print-config / --selftest / --help
```

Order of business: resolve config, create `$RESULTS/<timestamp>/`, record
`run.env` (resolved config, tool versions, `git rev-parse HEAD`, the exact
binaries and their mtimes), build unless told not to, run phase 0, then the
selected phases in numeric order, each as a *subprocess* so a phase that
dies cannot take the driver with it. A phase that exits nonzero without
having recorded a FAIL gets one synthesised, naming its exit code.

Phase 10 replaces phases 5, 8 and 9 rather than following them (the ADR:
"owns the disk for a session, replaces phases 5/8/9's scale"). Selecting
`--tb` without an explicit `--phase` list therefore runs 0-4, 6, 7, 10.

The driver guarantees a stopped daemon at the end of the run, whatever
happened, and writes `verdict.md` + `verdict.tsv` from the phase
`checks.tsv` files. `verdict.md` leads with the verdict line, then one
table row per phase (status, counts, wall time), then every FAIL and
FINDING in full with its evidence, then the SKIP list with reasons, then
the environment block. Exit 0/1/2 per component 1.

Acceptance: `--list`, `--print-config` and `--selftest` work with no store
at all; a run against a tempdir store produces a verdict whose exit code
matches the worst line in its own tables.

## Component 8: phases 1-4 (S3 functional, multipart, GC, RESP)

**`01-s3-functional.sh`** -- bucket lifecycle (create, double-create ->
`BucketAlreadyExists`, head, list); object CRUD at 1 B, inline-boundary
sizes either side of the configured threshold, 1 MiB - 1, 1 MiB, 1 MiB + 1
(the block boundary), 100 MiB and `QSSRT_SINGLE_PART_BYTES`; every GET
byte-compared with `cmp` against the generator; ETag compared against the
locally computed MD5 for every single-part object; `head-object`'s ETag
asserted present (component 0 says it is not: this is where that surfaces);
listings with `QSSRT_LIST_KEYS` > 1000 keys through **three** drivers --
the V2 paginator, `s3 ls --recursive`, and a manual continuation-token loop
-- each of which must enumerate every key, because a client that silently
sees 1000 of 1200 is the worst failure mode a listing has; prefix and
delimiter listings; `delete-objects` batches; error paths (`NoSuchBucket`
on list *and* on put to a name that was never created, `NoSuchKey`,
double-create). The inline boundary is measured, not assumed: the phase
records the block-file delta per PUT size and asserts that below-threshold
objects add no block file and above-threshold ones do.

**`02-multipart.sh`** -- `aws s3 cp` at `QSSRT_MULTIPART_BYTES` with the
part size pinned in the campaign's own `AWS_CONFIG_FILE` (so a chunk-size
default change in a future aws-cli cannot silently reshape the test), and
the tool version logged; manual `s3api` create/upload-part/complete with
parts **uploaded** out of order and completed in ascending order;
completion with a non-ascending part list rejected; the multipart ETag
verified against the MD5-of-MD5s convention with the `-N` suffix; abort
mid-upload, then `list-multipart-uploads` empty and the key absent;
`list-parts` and `list-multipart-uploads` against concurrent uploads; the
retryable failed complete (name a part that was never uploaded -> error,
upload survives, corrected complete succeeds). Every completed object is
byte-compared against the generator.

**`03-gc.sh`** -- and the one place the ADR asks for something the daemon
cannot do inside a campaign. The TTL is expressed in **days** and the sweep
period has a **one-hour floor** (`s3cas/src/main.rs`: `max(ttl/20, 1h)`),
so "wait out the TTL + sweep" is at minimum ~25 hours -- outside the 2-4
hour budget the ADR sets for a default run. Adding a sub-day TTL knob would
be a daemon change with no ADR, so this phase does what it can without one:
it asserts the GC's configuration from the daemon's own startup line, it
abandons uploads and asserts they are visible in `list-multipart-uploads`,
it asserts an explicit abort reclaims (disk usage back to baseline, part
records gone), and it reads the `s3_multipart_uploads_reaped` and
`s3_multipart_orphan_parts_reaped` counters. The sweep observation itself
is a SKIP with the arithmetic spelled out, unless `QSSRT_GC_WAIT=1` is set
-- then the phase really waits `ttl + period + slack` and asserts the
counters moved and the listing emptied. Flagged to the owner: the honest
fix is a duration-typed TTL, and that is an ADR-shaped decision.

**`04-resp.sh`** -- re-derives respd's dispatch surface from
`respd/src/cmd.rs` and fails if it differs from the pinned 20 (the ADR's
tripwire); then exercises it: namespaces (NSNEW/NSLIST/NSINFO/NSSET/SELECT
and the password path), SET/GET round trips at inline sizes and at the
largest value the server accepts, binary-safe payloads (embedded NUL, high
bytes, via `valkey-cli -x`), DEL/EXISTS/LENGTH/KEYTIME/CHECK, SCAN and
RSCAN cursor walks over more keys than a page, MGET, DBSIZE, TIME, PING,
wrong-arity and unknown-command errors, absent-key answers, pipelined
batches (`valkey-cli --pipe`), and concurrent clients. FLUSH runs last
because it empties the namespace.

Acceptance for all four: each phase re-runnable without manual cleanup, and
each one's failure messages naming the S3 error code or RESP reply that was
expected versus what arrived.

## Component 9: phases 5-7 (stress, crash, durability matrix)

**`05-stress.sh`** -- `xargs -P` workers over distinct keys sustaining mixed
put/get/delete for `QSSRT_STRESS_SECONDS`, plus the post-0008 assertions
that are the reason this phase changed shape:

- an **overwrite storm on one key**: N distinct contents PUT to the same
  key, then the key GETs byte-identical to the last content written, the
  live block-file count is exactly one object's worth (not N), and the
  blocks directory grew by one object's size within a block of tolerance --
  post-0008, a same-key overwrite storm leaves exactly the final object's
  occurrences and disk usage tracks live data;
- a **same-content re-PUT storm**: rc nets to unchanged, so the block-file
  count does not move at all;
- daemon RSS and fd count sampled from `/proc/<pid>` throughout, written as
  a TSV curve: fd count back to within a small delta of its start after a
  quiesce is a FAIL if it is not; RSS still climbing at the end is a
  FINDING with the curve attached;
- zero client errors across every worker (each worker's failures land in
  its own file; the phase folds them);
- then the daemon stops and a metadata-only fsck recount runs. **Post-0008
  the expectation is a clean report, not a leak-shaped one**: no crash has
  happened yet in this campaign, so `refcount_over_count` here is a
  FINDING with the count as evidence, not the expected background noise it
  was before ADR 0008.

**`06-crash.sh`** -- the crash cycle, repeated `QSSRT_CRASH_CYCLES` times
(default 3): start a multipart storm and a PUT storm, `kill -9` the daemon
at a randomized delay inside the storm window, restart, then verify that
**every operation the client saw succeed** is readable and byte-identical.
"Saw succeed" is a file: each worker appends `key<TAB>size` only after the
client command exited 0, so the post-crash verification set is exactly what
was acknowledged. Then fsck: findings restricted to the leak classes (zero
WARN, zero CRITICAL -- never an under-count, never corruption), `--repair`
runs, and a second fsck must converge. The residue counts per cycle go into
the verdict as a table, because their *volume* is the interesting number.

**`07-durability-matrix.sh`** -- the same crash cycle at
`durability = buffer`, in the default run. A `kill -9` is not power loss,
so the honest expectation is that the difference is residue volume and not
loss: any client-visible loss at either level is a FAIL and the campaign
says so. The phase writes the two levels' residue tables side by side.

The crash cycle itself lives in `tests/real/lib/crash.sh` and takes the
durability level as a parameter, so phases 6 and 7 cannot drift apart.

## Component 10: phases 8-9 (full scrub, teardown)

**`08-scrub.sh`** -- daemon stopped, `qss-storage-fsck --scrub` over the
fully populated store: zero corruption findings, and the throughput
recorded (bytes walked / wall seconds) as the ADR 0005 sizing estimate's
reality check. Throughput is recorded, never asserted: a number nobody has
measured yet is not a pass criterion.

**`09-teardown.sh`** -- delete everything through the clients: `s3 rm
--recursive` per bucket, `delete-objects` batches, bucket removal, respd
FLUSH; sampling the blocks-directory size as it drains so the reclaim curve
is a TSV, not an anecdote. Then the final fsck, and here the post-0008
expectation is absolute: **the store reconciles to empty**. Zero findings,
zero live block files, and the blocks directory back to within slack of its
size on an empty store. Anything left is enumerated with its class and
holder, which is the "or enumerate exactly what remains and why" the ADR
asks for.

## Component 11: phase 10 -- the terabyte

`10-terabyte.sh`, flag-gated behind `--tb`. Fills to
`min(QSSRT_TB_TARGET, 85% of the filesystem)` -- the cap is computed from
`df` at run time and is a refusal, not a truncation, if the disk cannot
hold the configured target.

Bands, each a function, each checkpointed:

| Band | Shape | Verification |
| --- | --- | --- |
| giants | 4 x 300 GiB, client-driven multipart | full |
| mid | ~1.5 TiB of 1-16 MiB objects | >= 1% sampled |
| hundred | ~150 GiB of 100 MiB objects | >= 1% sampled |
| tiny | ~3 GiB across 3,000,000 objects | >= 1% sampled |
| dedup | 300 GiB written twice under different keys | full, both copies |

Checkpoints live at
`$QSSRT_RESULTS_ROOT/tb-checkpoint/<seed>-<scale>/<band>.<shard>.done` --
outside the store (hard rule 1) and outside the timestamped run directory,
so a rerun resumes rather than restarts. Bands are sharded (default 1000
keys per shard) and a shard is stamped only when every key in it is
acknowledged; a resumed run re-does at most one shard per worker. The
per-key content is the generator's, so re-PUTting a key is idempotent by
construction.

The band-specific assertions:

- **dedup**: after both copies, the blocks directory holds ~300 GiB, not
  600 GiB, and the metadata-only fsck recount is clean. Post-0008 this is
  an equality claim, not a "pending reconciliation" one: rc is exact and
  there is nothing to reconcile.
- **tiny**: three million keys enumerated through paginated listings (all
  three drivers, as in phase 1), the metadata-only recount timed at 3M+
  objects, and the fjall DB size and daemon RSS recorded across the run.
- **one `kill -9` past 1 TiB**, at a randomized point, with resume: the
  fill continues, and the post-fill fsck shows leak-class residue only.
- **full scrub at capacity**, throughput recorded.
- **teardown at scale**: reclaim curve, and the final fsck reconciles to
  empty.
- **campaign-level**: no panic, no OOM, no fd exhaustion anywhere in the
  daemon log across the whole fill.

## Component 12: `make realtest`, `docs/realtest.md`, `.gitignore`

`make realtest` runs `tests/real/run.sh` with no phase selection (the
default campaign); `make realtest-tb` adds `--tb`; `make realtest-smoke`
runs the SCALE-divided tempdir smoke path. The Makefile stays a thin
forwarder -- the flags live in one place, `run.sh --help`.

`docs/realtest.md`: how to prepare the disk (mkfs.xfs, mount, chown, the
sentinel -- the owner's one-time work, explicitly outside the scripts), how
to run (default, `--tb`, single phases, `--resume`, `--fresh`), how to read
`verdict.md` and what each exit code obliges, what each phase proves and
what it does not (kill -9 is not power loss; the phase 3 sweep arithmetic),
and where the artifacts live.

`.gitignore`: `/target` already covers the results root; the entry that is
needed is for anything the harness drops beside itself (the generated AWS
config and credentials files live under the results dir, so nothing else
is required -- verify at implementation time rather than adding a
speculative rule).

## Sequencing and risk

Order: 0 -> 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 -> 8 -> 9 -> 10 -> 11 -> 12,
one commit per component, `bash -n` green on every script in every commit.

Riskiest is component 8, phase 1: it is the first phase to make a claim
about a real client's behaviour, and component 0 already knows three of its
assertions will fail against today's server. That is the campaign working
as designed, but it means the first real run's verdict must be legible
enough that the failures read as findings about the server and not as a
broken harness. Hence the evidence field on every check line, and hence the
three-driver listing test: "the V2 paginator stopped at 1000 while the
manual token loop got all 1200" is an actionable sentence; "listing failed"
is not.

Second riskiest is component 11: it is the only component that cannot be
exercised at all before the real disk exists. Its mitigation is that every
primitive it uses (generator, checkpointing, band workers, kill-and-resume)
is exercised by the smaller phases first, and that `SCALE` runs the same
code paths at a thousandth of the size.
