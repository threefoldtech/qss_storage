# The real-hardware validation campaign

`tests/real/` is ADR 0009 executable: a scripted, repeatable campaign that
drives qss_storage through real clients (aws-cli, valkey-cli) over a real
wire onto a real disk, and writes down a verdict. It is not CI. It consumes
the disk it runs on, it kills daemons on purpose, and a full run is hours.

Everything below assumes the repository root as the working directory.

## Preparing the disk (once, by hand, never by the scripts)

The campaign refuses to run anywhere but the designated hardware: an xfs
filesystem on its own NVMe, mounted at `/s3`, owned by the operating user.
The scripts never mkfs, never mount, never sudo. The owner's one-time work:

```
mkfs.xfs /dev/nvme1n1
mount /dev/nvme1n1 /s3            # and the fstab entry to keep it
chown $USER /s3
touch /s3/.qss-realtest           # the sentinel: proof of intent
```

The sentinel is what separates "this disk is for testing" from "this disk
happens to be mounted". Preflight fails without it and tells you the line
to run.

The campaign's own store lives at `/s3/qss-realtest/`, a directory under
the mount -- never the mount itself. That is what `--fresh` wipes, and the
mount is never an argument to a delete in any code path.

## Running

```
make realtest             # phases 0-9: the default campaign, ~2-4 hours
make realtest-tb          # phases 0-4, 6, 7, 10: the terabyte session,
                          # overnight; replaces 5/8/9 at scale
make realtest-selftest    # the harness testing itself; no disk needed
make realtest-smoke       # the harness against a tempdir at 1/4096 scale
```

Finer control through the entrypoint (flags live in one place --
`tests/real/run.sh --help`):

```
tests/real/run.sh --phase 5              one phase (preflight always runs)
tests/real/run.sh --from 6               everything from phase 6 on
tests/real/run.sh --fresh                wipe the campaign store first
tests/real/run.sh --resume               run against the store as it stands
tests/real/run.sh --scale 64             divide every size by 64
QSSRT_GC_WAIT=1 tests/real/run.sh --phase 3   actually wait out the GC (~25h)
```

The store precondition is deliberate: a non-empty store refuses to run
unless you say `--resume` (use what is there) or `--fresh` (wipe it, after
fsck confirms it is a qss store and nothing else lives in the directory).

Every size knob is an environment variable with its default in
`tests/real/campaign.conf`; the environment always wins. `--print-config`
shows the resolved values.

## Reading the verdict

Each run writes `target/realtest/<timestamp>/`:

```
verdict.md            the verdict: the fold, per-phase table, every FAIL,
                      FINDING and SKIP with evidence, measurements, env
verdict.tsv           the same fold, machine-readable
run.env               exact config, tool versions, git rev, binary stamps
phase-NN-name/        per phase: log, checks.tsv, measurements.tsv,
                      timing.tsv, fsck reports, daemon log slice
daemon/               the full daemon logs for the run
```

The exit code is the scripting contract:

- **0 PASS** -- every check passed. SKIPs are listed but do not gate.
- **1 FINDINGS** -- deviations worth filing, none of them loss or
  corruption. Read the Findings section of `verdict.md` and file them.
- **2 FAIL** -- a campaign pass criterion was violated: loss, corruption,
  a panic, a client-visible error where none is allowed, or hardware/rail
  refusal. The run is not evidence of health.

A run with `QSSRT_UNSAFE_ALLOW_ANY_PATH=1` (the smoke path) is stamped
**NOT CAMPAIGN GRADE** at the top of its own verdict: it exercised the
harness, not the disk.

## What each phase proves

| Phase | Claim |
| --- | --- |
| 0 preflight | the rail: mount, xfs, ownership, sentinel, tools, binaries, space, ports |
| 1 s3-functional | the S3 surface as aws-cli actually drives it: CRUD to 4 GiB, ETags, three listing drivers over >1000 keys, ranges, error paths, the on-disk layout |
| 2 multipart | client-driven and hand-driven multipart, out-of-order parts, the ETag convention, abort, the retryable failed complete |
| 3 gc | stale-upload GC: configuration, visibility, reclamation via abort; the sweep timer itself needs ~25h (below) |
| 4 resp | respcas's pinned 20-command surface, binary-safe, pipelined, concurrent |
| 5 stress | sustained mixed load; the ADR 0008 overwrite storms; fd/RSS curves; a clean recount with no crash having happened |
| 6 crash | kill -9 mid-storm at fsync, three cycles: acknowledged writes survive, residue is leak-class only, repair converges |
| 7 durability-matrix | the same cycles at `durability = buffer`: the difference must be residue volume, never loss |
| 8 scrub | every byte re-hashed: zero corruption; throughput recorded |
| 9 teardown | everything deleted through the clients; the store reconciles to empty |
| 10 terabyte | `--tb`: ~3 TiB in five bands, verified against the generator with no second copy anywhere; kill -9 past 1 TiB with resume; 3M-key listings; recount and scrub at capacity; teardown to empty |

## What this campaign does NOT prove

- **Power-loss durability.** The crash grade is `kill -9` and restart,
  full stop: process-crash atomicity (rename-before-record, claim
  atomicity, residue classes), which survives the page cache. What
  `durability = fsync` buys against actual power loss needs a power-cut
  rig or dm-flakey fault injection -- deliberately out of scope here
  (ADR 0009, answered open question: a separate machine, later).
- **The GC sweep timer, inside a default run.** The TTL is whole days
  (minimum 1) and the sweep period has a one-hour floor, so the first
  possible reap is ~25 hours after startup. Phase 3 asserts everything
  short of the timer and SKIPs the wait with the arithmetic; set
  `QSSRT_GC_WAIT=1` when you have the day. The honest fix would be a
  duration-typed TTL -- an ADR-shaped decision, flagged here on purpose.
- **Other filesystems, other disks.** `/s3`-only is the contract
  (answered open question). The rail enforces it.

## The moving parts, for the curious

`run.sh` drives numbered phases as subprocesses; each phase sources
`lib.sh` and speaks in check lines (PASS / FAIL / FINDING / SKIP) that
land in `checks.tsv`. The verdict is a pure fold over those files -- it
can be recomputed from the artifacts of a run that died halfway. Daemon
logs are captured whole and sliced per phase by byte offset, and any
daemon-side ERROR a phase did not explicitly declare fails that phase
regardless of what the client saw.

Object content everywhere is a pure function of the object key (an
AES-256-CTR keystream, seeded), so verification re-generates and compares
streams: the campaign never keeps a copy of what it wrote, which is what
makes the terabyte phase possible on one disk.
