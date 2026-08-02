# Campaign night 2026-08-02: the fsync loss that wasn't, the per-ack
# persist bill, and group commit's first real grades. In-tree copy of the
# operator report;
# raw runs, bench outputs and the four kept corpses live under
# target/realtest/ (not versioned): runs 20260801T233817 and
# 20260802T011036, bench-20260802T024309, loss-snapshots-*-leg[12]-*.

# qss_storage: the night of 2026-08-02

Campaign night report, written 03:05; headline section rewritten in
daylight after the corpses were decoded. Two full campaigns and five
bench runs on /s3 between 23:38 and 02:55, on development at a1a0ea2 --
the first campaign grade of the post-0011/0012 code with the per-ack
persist fix (a9b1587).

## Verdicts

| run | what | verdict |
| --- | --- | --- |
| 20260801T233817 | full campaign, shipped defaults (fsync, group_commit off), --fresh | **FAIL** - 2 phase-6 loss checks, since traced to false client acks (see headline) |
| 20260802T011036 | full campaign, group_commit = true (config copy), --fresh | **FAIL** - 2 phase-6 loss checks, same trace |
| bench-20260802T024309 | K=6 maxio x3 + single-stream A/B x2 | all fsck clean; numbers below |

Both campaigns fail on one criterion only: an acknowledged object
survives kill -9. Every other phase passed in both legs -- functional,
multipart, GC, RESP (the two standing ECHO findings, deviation by
design), the one-hour storm (64.6k / 64.3k ops, fd exactly steady at
15, RSS steady, overwrite storm leaving exactly one object's blocks),
and a spotless whole-store scrub in each leg.

## The headline, corrected: the fsync "loss" is a false client ack, not store loss

(The 03:05 version of this section accused the store: four lost
acknowledged multiparts at fsync, an ack-ahead-of-journal model to fit,
and an urgent fjall-rs report. The morning trace -- decoding the
corpses' journals entry by entry instead of grepping them -- reversed
the verdict. The check lines are real either way; what they mean
changed.)

**4 of 6 fsync crash cycles recorded one "lost" acknowledged multipart
each; 0 of 6 buffer cycles recorded any.** Both configs, default cycle
count:

| leg | cycle | flagged object | verifier |
| --- | --- | --- | --- |
| baseline | fsync-2 | mp-36 | RECORD ABSENT |
| baseline | fsync-3 | mp-71 | RECORD ABSENT |
| group commit | fsync-1 | mp-43 | RECORD ABSENT |
| group commit | fsync-3 | mp-30 | RECORD ABSENT |

Decoded (fjall 3.1.8 journal wire format: batches, seqnos, full keys --
not substring greps, which silently match other cycles' same-numbered
keys), all four corpses tell the same innocent story:

- the upload's CreateMultipartUpload record is persisted and present;
- some of its parts' block batches are committed (leg 2 cycle 1 even
  holds one part record -- parts upload in parallel and part 4 finished
  first); the remaining parts never arrived;
- no claim transaction, no object record: the CompleteMultipartUpload
  was never executed;
- both journals parse clean to EOF, batch seqnos contiguous -- no torn
  tail, no gap, nothing elided.

That is not a store losing an acknowledged write. That is a store
photographed mid-upload. The daemon never sent a 200 for those
completes -- in leg 2 cycle 1, three of the four parts were never even
ETagged. The acknowledgement exists only client-side: the harness
appends to acked.tsv when `aws s3 cp` exits 0, and for these four
uploads cp exited 0 for transfers it knew were unfinished.

The accusation is now a capture, not an inference. 60 idle-daemon
kill-at-random iterations produced zero false acks (target/ack-repro)
-- at idle the vulnerable window is milliseconds. Under the storm it is
not: the instrumented phase 6 (every mp cp's exit code, wall window and
stderr per key, QSSRT_CRASH_MP_LOG) caught it on its second run.
Run 20260802T100525, cycle 2, fsync-2/mp-80: all four part records
persisted -- they are the journal's final batches -- kill -9 lands
inside the CompleteMultipartUpload window, the complete never executes,
and `aws s3 cp` (aws-cli 2.36.14) exits 0 with zero bytes of stderr,
two tenths of a second after the daemon died. The harness recorded the
ack it was shown. The exit code was the lie.

The same decoder, pointed at LAST night's buffer-7/mp-68 corpse -- the
finding that reopened the fjall question -- found the same innocent
shape: its create record is the final batch in the journal; the upload
had just begun when the kill landed. The old "parts acked seconds
pre-kill absent from journals" constraint was the substring-grep
artifact. Both nights' losses were the same client bug.

What the correction restores and reopens:

- ADR 0006's fsync claim stands across kill -9: nothing the STORE
  acknowledged was lost tonight, in either leg.
- The fsync-vs-buffer "inversion" explains itself: at fsync an upload
  is in flight several times longer, so the random kill lands inside
  one far more often. Exposure time, not durability.
- Last night's buffer-7/mp-68 corpse has been re-read with the decoder:
  create-only-at-EOF, same innocent shape. The fjall question is
  CLOSED; docs/upstream/fjall-journal-ack-visibility.md now records the
  resolution.

Two smaller truths survive intact: the daemon's request log is
kill-truncated (stdout block-buffering -- its silence proves nothing
about anything), and the per-ack persist fix's REASON -- bare acks
never fsynced against POWER loss -- came from code reading and a
regression test, not from these corpses, and is untouched by their
reinterpretation.

## The per-ack persist fix's bill (the re-baseline the handoff asked for)

| leg | tonight | ledger (pre-fix) | delta |
| --- | --- | --- | --- |
| single-stream fsync, 16 GiB | 327 MB/s | 238 | night-to-night noise, read no trend |
| single-stream buffer, 16 GiB | 2355 MB/s | 2360 | flat -- the fix is free on a lone stream |
| K=6 fsync, 96 GiB | 1009 MiB/s | 2244 | **-55%** |
| K=6 buffer, 96 GiB | 928 MiB/s | 3224 | **-71%** |
| K=6 fsync + group commit | 769 MiB/s | -- | -24% vs tonight's fsync |

The shape matters more than the numbers. The fix costs nothing on a
lone stream at either level and takes half to two-thirds off parallel
ingest at BOTH levels -- fsync 1009 vs buffer 928 is durability no
longer mattering. That is not fsync's price; it is a convoy: one
persist call per ack-carrying insert serializing the journal writer
under concurrency. And kill -9 was never the thing it defends against
(the corrected headline: the store lost nothing across any kill,
before or after the fix); what it buys is power-loss honesty, which no
process-kill rig can grade.

All five bench stores came out fsck clean, the buffer crash leg
restarted fine, and the K=6 buffer store passed a full scrub.

## Group commit's first campaign (ADR 0011)

Functional parity everywhere, `s3_group_commits_degraded` 0 all night.
The performance story is genuinely mixed:

- The storm never feeds it: mean group size held at 1.05 through the
  whole hour (sampled at five points). An aws-cli-spawn-bound workload
  does not stack the commit station; the smoke A/B's 227->416 world
  (small objects, deep server-side concurrency) is where the groups
  are.
- One stream's concurrent parts: the 16 GiB phase-2 ingest went 910 ->
  1092 MB/s client-measured. The gain case exists.
- Six independent streams: 1009 -> 769 MiB/s (-24%). Batches near the
  cap gain nothing from grouping and pay the coupling.
- The finding worth a look before this ever leaves opt-in: after the
  16 GiB ingest, leg 2 spent ~2 minutes paying latency debt -- a 31s
  HeadObject, a 74s GET, then 2-3s per operation through the manual
  multipart section, all sub-second in the baseline leg at the same
  spots. The client got its acks early and everything behind the
  station paid afterwards. (Confound to rule out: a fjall compaction
  triggered by the bigger grouped transactions.)

## What actually got graded

Phase tables, both legs (pass/fail/finding/skip, seconds):

| phase | baseline | group commit |
| --- | --- | --- |
| 00-preflight | PASS 34/0/0/0, 2s | PASS 34/0/0/0, 1s |
| 01-s3-functional | PASS 63/0/0/0, 55s | PASS 63/0/0/0, 54s |
| 02-multipart | PASS 28/0/0/0, 81s | PASS 28/0/0/0, 175s |
| 03-gc | PASS 7/0/0/1, 4s | PASS 7/0/0/1, 4s |
| 04-resp | FINDINGS 34/0/2/0, 12s | FINDINGS 34/0/2/0, 12s |
| 05-stress | PASS 10/0/0/0, 3703s | PASS 10/0/0/0, 3703s |
| 06-crash | **FAIL** 14/2/0/0, 500s | **FAIL** 14/2/0/0, 489s |
| 07-durability-matrix | PASS 16/0/0/0, 646s | PASS 16/0/0/0, 565s |
| 08-scrub | PASS 3/0/0/0, 228s | PASS 3/0/0/0, 219s |
| 09-teardown | PASS 20/0/0/0, 292s | PASS 20/0/0/0, 287s |

Crash-cycle residue stayed in fsck's info class in all 12 cycles
(orphan block files and stale parts, the documented kill residue);
--repair was never needed beyond it. The ADR 0012 store identity
behaved: every --fresh store minted its id, every restart within a
cycle adopted nothing, fsck paired meta and blocks without complaint
all night.

## Decisions this report owes you

1. **CAPTURED -- now fix how the harness acknowledges, and tell
   aws-cli.** The false ack was caught live (fsync-2/mp-80 above): the
   reclassification of all six FAILs (both nights) as a client bug is
   no longer provisional. Two actions follow: the crash storm should
   record an mp ack only on evidence of the complete (s3api
   complete-multipart-upload's returned ETag, not cp's exit code) --
   ready to implement on your word -- and aws-cli deserves the
   upstream report fjall was about to get, with the mp-80 capture as
   the reproduction.
2. **The fjall-rs question is CLOSED.** Both nights' corpses decode to
   the same innocent shape, buffer-7/mp-68 included;
   docs/upstream/fjall-journal-ack-visibility.md now records the
   resolution and withdraws the upstream question. Only Finding 1's
   one-line documentation suggestion remains worth sending, at leisure.
3. **The per-ack persist fix (a9b1587) needs a scope decision, not a
   panic revert.** Its kill -9 justification evaporated with the
   correction; its power-loss rationale stands. It costs 55-71% of
   parallel ingest. The honest question is WHICH acks promise
   durability at fsync: multipart's part records and upload markers
   fail loudly on loss (InvalidPart / NoSuchUpload -- recoverable),
   while respd's SET/DEL have no later operation to catch a vanished
   write. That contract table is a small ADR; deciding it decides how
   much of the 55-71% comes back.
4. **Group commit stays opt-in.** Gain on one stream's parts, loss on
   parallel streams, and an unexplained post-ingest latency debt.

## Housekeeping

- Four corpses kept (fsync-cycle2/3 leg 1, fsync-cycle1/3 leg 2,
  ~119 MB total); all clean-cycle snapshots deleted per the harness's
  own protocol. Yesterday's buffer-cycle7 corpse untouched.
- The group-commit leg ran on a config COPY at
  target/realtest/qss_storage-realtest-groupcommit.toml (one delta:
  group_commit = true); the in-tree campaign toml still grades
  shipped defaults.
- Binaries at a1a0ea2 throughout, built once at 23:37; nothing new on
  the branch tonight but this report. Nothing pushed.
- Morning-trace artifacts: the journal decoder and the idle-daemon
  false-ack reproducer live in the session scratchpad (dump-journal.py,
  ack-repro.sh; results at target/ack-repro); the capture run and its
  audit tsv at target/realtest/loss-snapshots-20260802T100525-ackaudit-r2;
  the crash_multipart_worker instrumentation (QSSRT_CRASH_MP_LOG) is
  committed alongside this revision.
- Standing queue: the aws-cli upstream report (decision 1), the harness
  ack-criterion fix (decision 1), the per-ack persist scope ADR
  (decision 3), the tiered-store harness rail (ADR 0012 follow-up),
  ADR 0004, and the 3 Dependabot alerts (1 high).

---

## Addendum, the afternoon of 2026-08-02: ADR 0013, and the drive that cried wolf

Written 15:40, after the owner's "do that with the small adr and code it,
rerun the campaign after".

### What landed

ADR 0013 (the ack durability contract) written, accepted, implemented:
bare acks split into a contract class (object records, bucket metadata,
respd SET/DEL -- persist at configured durability, their loss would be
silent) and a recoverable class (`_MULTIPART_PARTS`, `_UPLOADS` -- no
explicit persist; a power cut answers with InvalidPart / NoSuchUpload,
loud and retryable, leak-class at worst). Both classes stay
kernel-visible before the ack; the journal-bytes regression tests now
pin that for the recoverable trees specifically. The crash storm's mp
client became s3api-driven and acknowledges ONLY on the complete's
returned ETag -- the exit-code criterion that manufactured both nights'
"losses" is gone.

### The campaign, rerun (20260802T115127)

**VERDICT: FINDINGS (exit 1)** -- zero FAILs anywhere; the only two
findings are respd's documented ECHO deviations. All 12 crash cycles
clean at both durability levels under the honest ack criterion, storm
64.5k ops with fd/RSS steady, scrub spotless. The first full campaign
since the false-ack bug was removed from the harness, and the store's
fsync claim held everywhere it was tested.

### The bench saga, and its resolution

| run | K=6 fsync | K=6 buffer | conditions |
| --- | --- | --- | --- |
| ledger (night 1, pre-0011 code) | 2244 | 3224 | drive state unknown, cool |
| night 2 (post per-ack persist) | 1009 | 928 | TRIM-starved, unknown then |
| post-0013, before trim | 576 | 748 | starved further |
| minutes after `fstrim` (3.6 TiB) | 475 | 854 -> 646 | FTL digesting the discards |
| post-trim + 30 min idle | **2214** | **3318** | clean FTL, 70-74C |

The collapse was the FTL all along. `rm -rf` discards nothing; ~5 TiB
of fill-and-wipe cycles since the terabyte run starved the T700's
flash translation layer, and the "-55% / -71% per-ack persist cost"
in this report's own re-baseline section was measured on that starved
drive -- the magnitude was mostly the disk, not the code. An ABA run
(pre-0011 binaries vs post-0013, interleaved same-hour: 801 / 689 /
646, declining monotonically through both versions) exonerates every
code change including 0011/0012-with-group-commit-off. Temperature is
a red herring: the parity numbers finish at 74C, the same reading the
"throttled" runs showed. What survives of the convoy story is night
2's fsync~=buffer flattening -- real evidence that the per-ack persist
serialized the journal writer -- but its true price was never cleanly
measured and, post-0013, no longer exists to measure.

**ADR 0013 acceptance: met.** Campaign clean (the ECHO finding floor
only) and K=6 at statistical parity with the pre-fix ledger at both
durability levels, with the contract kept where loss would be silent.

### Operational actions this hands you

- Enable periodic TRIM on /s3 (`fstrim.timer`, weekly) or mount with
  `discard=async`. This afternoon is what three weeks of campaigns
  would otherwise do to the drive, permanently.
- Benches now bracket drive temperature (smartctl); folding a
  temperature + trim-state line into campaign preflight is queued for
  the harness.
- The cold-morning K=6 numbers (first load of the day, FTL clean and
  drive at ambient) are worth collecting once, opportunistically --
  they are the only ledger row this report could not produce.

### Standing queue after this addendum

The aws-cli upstream report (the mp-80 capture); respd ECHO (fifteen
lines, turns the campaign's last two findings into passes); the
tiered-store harness rail; ADR 0004; the 3 Dependabot alerts; and
fstrim.timer above.

**Closed 2026-08-02 (later the same day):** ECHO landed, and it was not
fifteen lines. ECHO alone would not have turned those two findings into
passes: `valkey-cli --pipe` passes its stdin through untouched, so its
commands arrive as inline text and its terminator is a bare CRLF before
the ECHO frame -- neither of which the parser accepted. Inline commands
are parsed now, a malformed frame is answered and hung up on instead of
silently wedging the connection, and the daemon was renamed `respcas`
in the same series. Phase 04 grades --pipe as a violation from here on.
