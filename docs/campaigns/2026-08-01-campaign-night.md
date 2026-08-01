# Campaign night 2026-08-01: bugs, verdicts, and the bandwidth story.
# In-tree copy of the operator report delivered that morning; raw
# sampler data and rig outputs live in
# target/realtest/night-2026-08-01-artifacts/ (not versioned).

# qss_storage: the night of 2026-08-01

Campaign night report, written 08:23 while the terabyte run fills its
last band. Everything below happened between 23:18 and now.

## Verdicts

| run | what | verdict |
| --- | --- | --- |
| 20260801T003436 | smoke (1/4096, tmpfs) | CLEAN - 0 fail, 2 by-design findings |
| 20260801T034032 | full campaign on /s3, rail intact | **CLEAN** - 0 fail, 2 by-design findings |
| 20260801T055614 | terabyte (--tb), in flight | 1 real finding so far, see below; ETA ~11:00 |

The campaign verdict is the ADR 0009 acceptance statement: at full scale
on the designated hardware, every phase passed. Every acknowledged write
survived every kill -9 at fsync durability, byte-identical. The two
standing findings are respd's documented lack of ECHO (valkey-cli
--pipe cannot terminate its stream), graded deviation by design.

## The one open product question

**An acknowledged multipart complete at BUFFER durability vanished
across kill -9** (`buffer-3/mp-94: RECORD ABSENT`, terabyte run phase
7). The upgraded verifier proved the record is gone - not a read
hiccup. fsync mode never lost anything all night (26 crash cycles
across the runs). The implication: buffer-mode acks sit in userspace
memory, not in the page cache - a plain write() would survive process
death. Whether buffer mode's contract permits that is your call; it
smells like a one-line flush-without-fsync fix if it is not.

## Product bugs fixed (all on development, NOT pushed)

| commit | fix |
| --- | --- |
| 116d5e4 | ranged GET: headers/body/status from one arithmetic; real 416s; suffix ranges; block-boundary off-by-one in BlockStream; verify only whole reads |
| 3a64bdc | ListObjectsV2 is_truncated (the CLI paginator stopped at 1000 keys); v1 next-marker skipped a key per page; max-keys panics; list handlers no longer create ghost buckets |
| 22af5d1 | respd DEL counts what existed and is variadic per Redis |
| 2838f63 | blocks DB moved blocks/db -> blocks/.db: the 0xdb fanout collision made 1/256 of blocks invisible to fsck; legacy stores refused at open |
| ad0eefa | HeadObject carries ETag + Accept-Ranges; respd logs client faults at WARN not ERROR |
| 1ca9b9b | delimiter/CommonPrefixes in both list handlers, group-consuming pagination, SDK-verified |

## Harness bugs fixed

| commit | fix |
| --- | --- |
| 7efd86f | phase 4 hung forever: bare `wait` included the respd daemon |
| ad0eefa | gen_stream failed under pipefail (14k phantom PUT errors); rand_between died under set -u (every kill -9 landed at +0s); aws-cli "None" counted as a phantom upload per bucket; scale-blind range window; FLUSH tested where it is actually allowed |
| ccead0f | s3_get_stream fifo shared by all workers via $$ - deadlocked the storm |
| 33bd441 (unsigned) | crash storm uploaded sibling workers' bytes: put-$$-$RANDOM with forked-identical RANDOM state; verifier now says RECORD ABSENT vs record present |
| b3da6dc (unsigned) | --tb sequence reached the crash phases with phase 1's daemon still on the port; phases 6/7 now adopt-and-stop at entry |

The two unsigned commits happened after your gpg agent's cache expired
(pinentry timed out at 03:45 with nobody at the keyboard). `git rebase
--exec 'git commit --amend --no-edit -S' 33bd441^..` or just leave them.

## The three campaign attempts

1. **00:56** - killed at minute 52 of the storm by the session harness
   stopping my background wrapper; the process tree died with it. All
   clean until the kill. Campaigns run setsid+nohup+disown since.
2. **01:54** - FAILED phases 6/7: 52 "corrupted" objects at fsync. The
   store was innocent: every read-back hash was the generated hash of a
   sibling worker's key (w1/N held w2/N's bytes) - the shared tmp path
   above. Proven by recomputing the generator tables offline.
3. **03:40** - CLEAN. 247 pass, 0 fail, the finding floor only.

Then terabyte attempt 1 (05:33) hit the entry-hygiene bug within its
crash phases; attempt 2 (05:56) is the one running now.

## Bandwidth

Client-visible: the 60-min storm ran ~100 MB/s and is aws-cli
process-spawn bound by design (one Python CLI per op); single streams
move much faster - phase 2 multipart at 381 MB/s client-measured.

Store-side, terabyte run (10s sampler on nvme1n1, 147 min so far):

- mean 194 MiB/s, p50 183, p90 362, p99 377, peak 401 MiB/s write
- disk utilization pinned at ~99% through the giants band
- 1.67 TiB written by 08:23; store at 1.6 TiB of the 3.1 TiB cap
- giants band (4 x 300 GiB, client multipart): 348 -> 203 -> 177 ->
  171 MB/s per object - a gentle decline as the block DB deepens,
  worth a look but not a cliff
- reads ~0 during ingest: verification regenerates content instead of
  re-reading references, so the read side stays out of the way

sar (sysstat, enabled tonight as systemd timers per your ask) has the
10-minute persistent record at /var/log/sa/ from ~01:20 onward; the
fine-grained TSV is tb-io.tsv next to this report.

## Housekeeping

- sysstat.service + collect/rotate/summary timers enabled system-wide.
- Nothing pushed anywhere; ten local commits await your review.
- The tb run finishes ~11:00: its verdict, the reclaim curve, and the
  final bandwidth numbers land in an addendum when it closes.
