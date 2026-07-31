# qss-storage-fsck: offline reconciliation, scrub and repair

`qss-storage-fsck` is the reconciliation half of the refcount contract
(`docs/refcount.md`). That contract is asymmetric on purpose: refcounts may
over-count ("leakage") but must never under-count ("loss"). Since ADR 0006
the run-time half holds unconditionally -- every block-record mutation is a
transactional read-modify-write under the block's stripe, and block files
are durable at their final path before their record commits. Nothing at run
time ever recounts, reclaims or repairs, so every accepted-leakage path
accumulates until something walks the store and reconciles it. That
something is this tool.

Its home is ADR 0005 (`docs/adr/0005-fsck-scrub-reconciliation.md`). The
walkers, passes and repair actions are library code in
`cas-storage`'s `scrub` module; the binary is the CLI, the store open and
the exit-code contract.

## When to run it

- **Routinely**, to reclaim leaked references. The same-key overwrite leak
  is not crash-only: `create_object_meta` blind-upserts the object record
  and decrements nothing for the object it replaced, so every overwrite
  leaks the replaced object's refcounts by design. Without a recount,
  `blocks/` only ever grows.
- **After a crash or a hard power cut**, especially at
  `durability = "buffer"`, where a record can survive while its file's
  pages do not. An unrepaired dangling record actively poisons dedup: every
  later PUT of the same content bumps the fileless record, skips the write,
  and commits another damaged object.
- **Periodically with `--scrub`**, to verify cold data. Verify-on-read (ADR
  0002) is opt-in and whole-object only, so data nobody reads is data
  nobody checks.
- **Before trusting a store you did not write** -- after a restore, a
  migration, or a manual poke at `blocks/`.

## Exclusivity

fsck is offline. Exclusivity is inherited rather than built: fjall's LOCK
file makes this tool and a running daemon mutually exclusive on every
database fsck opens, so a store the daemon still has open fails at the open
(exit 3). That is the intended answer -- stop the daemon first.

The residual hole is ADR 0004's (store ownership, still Proposed): a second
process pointed at a *different* meta root over the *same* `blocks/` tree
is not excluded by anything, because the two processes never touch the same
database. Do not do that; fsck cannot detect it.

Opening the store also runs the store's own open duties for free before the
first walk: `blocks/.tmp` is purged wholesale (crash residue is garbage by
definition) and the temp dir is re-checked for being on the same filesystem
as the blocks root.

## Running it

```
qss-storage-fsck [--config <file>] [--meta-root <dir>] [--fs-root <dir>]
                 [--metadata-db <engine>] [--durability <level>]
                 [--inline-metadata-size <n>]
                 [--scrub] [--repair] [--json]
```

`--meta-root` and `--fs-root` default to `.`, the same defaults the server
uses. The layout they name:

```
<meta_root>/db/              namespace DB: the bucket (object) trees
<meta_root>/store_header.bin sidecar copy of its header
<meta_root>/blocks/db/       shared blocks DB: _BLOCKS, _MULTIPART_PARTS
<meta_root>/blocks/store_header.bin
<fs_root>/blocks/            block data files (fanout dirs, full-hex names)
<fs_root>/blocks/.tmp/       write staging, purged at open
<fs_root>/blocks/.quarantine/ what a --repair set aside
```

`--metadata-db`, `--durability` and `--inline-metadata-size` are resolved
through the same CLI-over-config-over-defaults merge every other tool uses
(`--config`, else `./qss_storage.toml`, else
`/etc/qss_storage/qss_storage.toml`). They must match how the daemon opens
the store; a tool that opens it with a different backend or inline
threshold reads the wrong thing. Which config file was picked up is printed
to **stderr**, so `--json` on stdout stays machine-clean.

`verify_on_read` is forced off whatever the config says: fsck never reads
an object through the read path, and `--scrub` re-hashes every block
itself.

**fsck never creates a store.** Both databases are classified before
anything is constructed, and a path that is not already a store is refused
with `no store at <path>` and exit 3. The opposite behaviour -- creating an
empty store at a mistyped `--meta-root` and reporting it clean -- is the
loudest possible wrong answer from a tool whose job is finding damage.

## The passes

Passes appear in the report's `passes_run` in the order they ran. A pass
that is absent did not run, which is information: it is how a consumer
tells "the recount found nothing" from "the recount refused".

| Pass | Name in the report | What it compares |
| --- | --- | --- |
| Refcount recount | `recount` | holder references vs `_BLOCKS` |
| Disk sweep | `disk_sweep` | block files vs the records naming them |
| Dangling sweep | `dangling_sweep` | each record vs the file it names |
| Corruption scrub | `corruption_scrub` | each file's bytes vs its own name (`--scrub` only) |
| Multipart report | `multipart_report` | in-flight uploads, grouped |
| Bucket integrity | `bucket_integrity` | object trees vs `_BUCKETS` rows |

### 1. Refcount recount

Walks every holder and counts block references **per occurrence** (the
counting rule of `docs/refcount.md`: a block listed twice in one object
counts twice). Two holder classes: object records in every non-reserved
tree of the namespace DB, and part records in `_MULTIPART_PARTS`
unconditionally -- with ADR 0003 unimplemented, part records are the only
holders of an in-flight upload's blocks, and skipping them would recount a
live upload to zero.

**The closed-holder-set rule.** A recount is only meaningful over the
complete holder set. Any incompleteness -- a tree that will not open, an
iteration step that fails, a single record that will not decode -- aborts
the enumeration: the recount does not run, is absent from `passes_run`, and
the refusal is itself a CRITICAL finding (`holder_enumeration_failed`).
Every other pass still reports. Under `--repair` the absence of `recount`
forbids every refcount mutation, in either direction. A recount over a
partial holder set that then "repairs" frees live blocks: loss by repair,
the one failure mode this tool must structurally exclude.

Findings: `refcount_over_count` (INFO -- the expected direction),
`refcount_under_count` (CRITICAL), `missing_block_record` (CRITICAL: a
holder references a block with no record at all).

### 2. Disk sweep

Walks `<fs_root>/blocks/` recursively, classifying every entry by structure
alone -- no DB lookup. Under the ADR 0006 layout a block file's name is the
full lowercase hex of its own address and the directories above it spell
that address's prefix, so acceptance is a round-trip test: for an accepted
`(id, depth)`, `block_disk_path(id, depth, root)` is the path it was found
at. A name that parses as hex but sits under someone else's prefix, or
directly under the root, is unreachable by any reader and therefore
foreign, not misplaced.

`.tmp` and `.quarantine` are skipped **at the top level only**; deeper down
a directory by either name is not a fanout level and is reported foreign
like anything else.

Findings: `orphan_file` (INFO: no record), `off_depth_file` (INFO: a copy
at a depth the record does not name, while the record's own file is where
it says), `foreign_file` (WARN), `size_mismatch` (CRITICAL: the file at the
record's own depth is not the size the record describes -- a corruption
tell that costs one stat, no read).

One artifact, one finding: a misplaced file whose record has no file of its
own is *not* an off-depth duplicate. It is the adoption candidate the
dangling sweep owns, and reporting it from both ends would force repair to
join findings to work out that there is only one file.

### 3. Dangling sweep

One stat per record, at the record's own depth. A miss means one of two
very different things:

- the id exists on disk at another depth -> `adoptable_dangling_record`
  (INFO). Repair re-hashes that copy before pointing the record at it.
- the id is nowhere -> `dangling_record` (CRITICAL). The bytes are gone,
  every holder is damaged, and the record poisons dedup until repaired. The
  finding carries the **blast radius**: a targeted second holder walk fills
  in every object and part that references the block, so the operator sees
  what a repair will affect before running one.

A record already flagged degraded is reported as `degraded_record` (INFO):
known damage awaiting a heal, not a fresh discovery.

### 4. Corruption scrub (`--scrub`)

Reads every block file and re-hashes it against the address it is filed
under, with the store's own hasher (taken from the header). Self-attributing:
the file's name is the expected hash, so no record lookup is needed. A file
that cannot be read is reported corrupt too -- the operator's next move is
the same, and swallowing it would let an unreadable block pass for a
verified one.

Finding: `corrupt_block` (CRITICAL).

This is the disk-bound pass: roughly store size divided by sequential read
rate (a 4 TB store at 500 MB/s is about 2.5 hours), and the only pass that
is opt-in. Everything else finishes in one pass over the metadata.

### 5. Multipart report

Groups `_MULTIPART_PARTS` by (bucket, key, upload_id) and reports part
count and total bytes held per upload as `multipart_upload` (INFO).
Report-only until ADR 0003 lands: reaping needs 0003's abort semantics,
which decrement through the striped delete primitive, and guessing them
here would smuggle 0003 in.

**No age is reported.** A part record carries no timestamp; upload records
arrive with ADR 0003. Every finding says so in its evidence.

A part record that will not decode is the same refusal the holder walk
makes -- a part record *is* a holder -- so it surfaces as
`holder_enumeration_failed`.

### 6. Bucket integrity

`bucket_delete` removes the `_BUCKETS` row before tearing objects down, so
a crash mid-loop strands an invisible object tree whose records still hold
block references. Every non-reserved tree with no bucket row is
`half_deleted_bucket` (WARN), sized with its object count.

The recount stays truthful either way, because holder enumeration walks
*trees*, not bucket rows (see "as built" below). This is an inconsistency
to resume, not a loss.

## Findings and severities

Severity is a property of the class, in one place, so the table below is
the whole story.

| Class | Severity | Meaning |
| --- | --- | --- |
| `refcount_over_count` | INFO | rc above the walked holder count: leaked references |
| `orphan_file` | INFO | a block file no record references |
| `off_depth_file` | INFO | a redundant copy at a depth the record does not name |
| `adoptable_dangling_record` | INFO | the record's file is missing but the id is on disk elsewhere |
| `degraded_record` | INFO | known damage, flagged, awaiting a heal |
| `multipart_upload` | INFO | an in-flight upload and what it holds |
| `foreign_file` | WARN | something under `blocks/` that is not this store's layout |
| `half_deleted_bucket` | WARN | an object tree with no `_BUCKETS` row |
| `refcount_under_count` | CRITICAL | rc below the holder count: a premature free waiting to happen |
| `missing_block_record` | CRITICAL | a holder references a block with no record |
| `undecodable_block_record` | CRITICAL | a `_BLOCKS` record that does not decode |
| `size_mismatch` | CRITICAL | the file is not the size its record describes |
| `dangling_record` | CRITICAL | the bytes are gone and nothing on disk can replace them |
| `corrupt_block` | CRITICAL | bytes that do not hash to the address they are filed under |
| `holder_enumeration_failed` | CRITICAL | the holder set could not be closed; no recount ran |
| `post_repair_recount_dirty` | CRITICAL | something this tool claims to repair survived `--repair` |

INFO is not "ignore me", it is "nothing you must act on today". Exit 0 with
findings is normal and expected on a busy store.

## Exit codes

The scripting contract. Once shipped, these do not move.

| Code | Meaning |
| --- | --- |
| 0 | Clean, or findings no worse than INFO |
| 1 | At least one WARN, no CRITICAL |
| 2 | At least one CRITICAL |
| 3 | Could not run at all |

Exit 3 never appears inside a report: if the store could not be opened or
walked there is no trustworthy report to put it in, so the binary prints
the reason to stderr and exits 3 directly. That is how a script tells a
store it could not check from a store it checked and found damaged.

With `--repair`, the process exits with the **post-repair** report's code.

## report -> repair -> recount

1. **Report.** Every pass runs and the report is written to stdout **and
   flushed** before the first repair action. A `--repair` that dies halfway
   still leaves the operator the evidence.
2. **Repair.** `--repair` applies the safe subset (below). Every action is
   idempotent -- rc is *set* to a target rather than adjusted by a delta,
   unlinks and renames tolerate ENOENT, the degraded flag is set rather
   than toggled -- so a killed `--repair` is re-run, not recovered. There
   is no recovery procedure; re-running fsck is the recovery procedure.
3. **Recount.** After the last action every pass runs again, with the same
   options the report was produced with. Anything this tool claims to
   repair that is still standing becomes `post_repair_recount_dirty`
   (CRITICAL) whatever that finding's own severity is: a leftover orphan
   file is INFO on its own, but a leftover orphan file after a repair that
   said it would delete it is a repair that did not work. The classes
   nobody repairs -- `missing_block_record`, `undecodable_block_record`,
   `size_mismatch`, `degraded_record`, `multipart_upload` and
   `holder_enumeration_failed` -- are expected to survive and say nothing
   about whether the repair worked, though their own severity still sets
   the exit code.

Report-first is not a formality. A bug in a repair action becomes a
data-eating bug on first contact with a damaged store, which is precisely
when trust is lowest. Read the report. `--repair` is a second command.

### What `--repair` does

Actions run in a fixed order, and the order is load-bearing.

| Action | What it does |
| --- | --- |
| `resume_bucket_teardown` | Deletes every object of a stranded tree through the daemon's own striped delete path (so refcounts fall exactly as the crashed teardown would have dropped them), then drops the tree. A non-UTF-8 object key cannot go through that path: it is reported WARN and skipped, the tree is still dropped, and the recount reconciles what it was holding. |
| `quarantine_corrupt` | Moves a file whose bytes do not hash to its own name into `blocks/.quarantine/` **and** marks its record degraded. Both halves or neither: a quarantined file with a live record leaves dedup pointing at nothing, and a degraded record over corrupt bytes leaves the bytes where a reader still finds them. |
| `adopt_orphan` | Re-hashes the candidate copy first (fsck does not have the writer's bytes, so unlike the write path's heal it must verify before trusting), then points the record's depth at it and deletes the remaining copies. A candidate that does not hash is corrupt residue: quarantined. If every candidate fails, the record is marked degraded. |
| `set_rc` | Sets rc to the walked holder count, in both directions. Gated on the recount having run. |
| `mark_degraded` | Flags a record whose bytes are nowhere. Accounting stays honest, poisoning stops. |
| `delete_orphan` / `delete_off_depth` | Deletes files nothing can reference. Last, so nothing above still wants them. |
| `quarantine_foreign` | Moves what is not part of the layout out of the blocks root. Never deleted: fsck did not put it there and does not know what it is. |

Two rules worth stating separately:

- **rc is raised as well as lowered.** Lowering to the walked truth is safe
  under exclusivity; raising an under-count is the conservative direction
  and defuses a live premature-free landmine. The CRITICAL finding, its
  evidence and the nonzero exit all remain, so the bug it indicates is not
  hidden.
- **rc := 0 frees the block.** The protocol has no representable rc=0
  state, so `set_rc` to zero does what the last decrement would have done:
  the record is removed in a transaction and the file is unlinked, record
  first, so a crash in between leaves an orphan file (leakage) rather than
  a record with no bytes (loss).

No stripes are taken during repair, deliberately: the stripes serialize
concurrent daemon writers, and fsck holds the store's fjall LOCK, so there
is no second writer. Record mutations still go through transactions --
atomicity against a crash is a different property from exclusion against a
peer, and only the first one is free.

## Quarantine

`--repair` moves rather than deletes anything it cannot prove is
unreferenced garbage. Destination: `<fs_root>/blocks/.quarantine/`.

- A filesystem quarantine rather than a DB tree: it is visible with `ls`,
  it survives metadata damage, and a block file's full-id name stays
  self-identifying wherever it sits.
- The directory is created and fsynced on use, so a quarantine survives the
  power cut a corruption finding often precedes.
- A name already taken gets a numeric suffix (`<name>.1`, `<name>.2`, ...)
  rather than overwriting what an earlier run set aside.
- **fsck never deletes anything from quarantine.** Emptying it is an
  operator decision, taken after looking at what is in there.
- The disk walk skips `.quarantine` at the top level, so quarantined files
  are not re-reported as orphans on the next run.

### The store's own files are not foreign

In the default single-root layout (`--meta-root . --fs-root .`) the shared
blocks database and its header sidecar live at `<meta_root>/blocks/db` and
`<meta_root>/blocks/store_header.bin` -- which is *inside* `<fs_root>/blocks`,
the tree the disk walk walks. The walker skips the store's own paths,
declared by the opener (the binary knows which paths it opened; the library
does not guess). Without that, every run would report the live database as
a foreign file, and `--repair` would rename it into quarantine.

## The degraded flag

Record format v3 (ADR 0005) adds a flags byte to the block record; bit 0 is
`degraded`. It exists to make "heal with honest accounting" expressible,
and it is the only part of ADR 0005 that touches the daemon.

**What sets it.** Only fsck's repair: `mark_degraded` on an un-adoptable
dangling record, `quarantine_corrupt` on a block whose bytes do not hash,
and `adopt_orphan` when every candidate turns out to be corrupt. Setting it
is idempotent.

**What it does at run time.** A degraded record is *absent for dedup*: the
write path's bump RMW sees the flag and reports a miss, so the PUT falls
through to the insert path, writes and fsyncs the file, and then -- in the
same transaction, under the same stripe -- clears the flag, takes the depth
the file actually landed at (the record follows the file; the heal's
placement need not match what the dead record named), and adds its own
reference to the rc that was already accounting for the surviving holders.
Holders heal permanently, rc never lies, and the poisoning stops.

Deletes need no special case: a degraded record decrements like any other,
and the unlink after its last reference tolerates ENOENT -- having no file
is exactly what degraded means.

**What fsck says about one.** `degraded_record`, INFO, naming the rc still
accounted for and whether a file is now present at the path (if one is, the
next PUT of that content will heal it). Degraded records are excluded from
the dangling classification and from repair planning; a recount can still
change their rc, and a recount to zero frees them like any other record.

The alternative -- removing the record and letting dedup heal it -- was
rejected: the surviving holders would make the healed record's rc=1 a lie,
and the lie breaks in the worst direction. An object that read fine
yesterday, incidentally healed, would break tomorrow when the new writer's
delete freed the block, with no operation performed on it. Silent future
breakage is strictly worse than present, documented damage.

## JSON

`--json` renders the same documents as machine input. Both carry a
`version` field: this is the *document's* schema version, not the store's.
Adding a field is not a bump; changing or removing one is. Both are at
version 1.

### Report

```json
{
  "version": 1,
  "store": { "blocks_root": "/data/blocks", "meta_root": "/data" },
  "passes_run": ["recount", "disk_sweep", "dangling_sweep",
                 "multipart_report", "bucket_integrity"],
  "findings": [
    {
      "severity": "critical",
      "class": "dangling_record",
      "block": "ab...ff",
      "path": "/data/blocks/ab/ab...ff",
      "evidence": "no file at depth 1, and the id is nowhere else on disk ...",
      "holders": [
        { "kind": "object", "bucket": "photos", "key": "cat.png" },
        { "kind": "part", "bucket": "photos", "key": "big",
          "upload_id": "u-1", "part_number": 2 }
      ]
    }
  ],
  "summary": { "info": 3, "warn": 1, "critical": 1, "total": 5 },
  "exit_code": 2
}
```

- `store.meta_root` is absent when the caller did not supply one.
- `block`, `path` and `holders` are omitted when empty rather than emitted
  as nulls -- test for presence, not for a null.
- `evidence` is free text for a person. Scripts key off `class`, `severity`
  and the typed fields.
- `findings` is sorted worst first, then by class, block, path and
  evidence, so two runs over an unchanged store produce byte-identical
  reports. A report that reshuffles itself cannot be diffed.
- `severity` is one of `info`, `warn`, `critical`; `class` is one of the
  classes in the table above, spelled identically in the text render.

### Repair summary

Emitted after the report, so `--repair --json` writes **two JSON documents**
to stdout in sequence: the pre-repair report, then the summary (which
embeds the post-repair report whole, under `report`).

```json
{
  "version": 1,
  "outcomes": [
    {
      "action": "quarantine_foreign",
      "status": "applied",
      "severity": "info",
      "path": "/data/blocks/NOTES",
      "detail": "moved out of the blocks root to ..."
    }
  ],
  "counts": { "applied": 1, "skipped": 0, "failed": 0, "total": 1 },
  "report": { "version": 1, "...": "the post-repair report" }
}
```

- `action` is the action name from the repair table above.
- `status` is `applied`, `skipped` or `failed`. Applied and skipped work is
  `info`; a partial action (a skipped non-UTF-8 key, refcounts left alone
  because the holder set was not closed) is `warn`; a failure is
  `critical`.
- `block` and `path` are omitted when the action was about neither.
- The run's exit code is `report.exit_code` of the nested post-repair
  report, not a field of the summary.

## Examples

Report only, text, on a store at `/var/lib/qss_storage`:

```sh
qss-storage-fsck --meta-root /var/lib/qss_storage --fs-root /var/lib/qss_storage
```

Add the disk-bound verification pass (hours on a large store):

```sh
qss-storage-fsck --meta-root /srv/qss --fs-root /srv/qss --scrub
```

Read the report, then repair:

```sh
qss-storage-fsck --meta-root /srv/qss --fs-root /srv/qss           # look
qss-storage-fsck --meta-root /srv/qss --fs-root /srv/qss --repair  # act
```

Machine-readable, one line per finding:

```sh
qss-storage-fsck --meta-root /srv/qss --fs-root /srv/qss --json \
  | jq -r '.findings[] | "\(.severity)\t\(.class)\t\(.block // .path // "-")"'
```

Just the verdict, for a cron job:

```sh
qss-storage-fsck --meta-root /srv/qss --fs-root /srv/qss --json \
  | jq '{exit: .exit_code, summary: .summary}'
```

Both documents of a repair run (jq reads the concatenated stream; `-s`
collects them into an array):

```sh
qss-storage-fsck --meta-root /srv/qss --fs-root /srv/qss --repair --json \
  | jq -s '{before: .[0].summary, actions: .[1].counts,
            after: .[1].report.summary}'
```

Exit code in a script:

```sh
qss-storage-fsck --meta-root /srv/qss --fs-root /srv/qss --json > report.json
case $? in
  0) ;;                                   # clean, or informational only
  1) echo "inconsistencies, see report.json" ;;
  2) echo "loss or loss-risk, see report.json"; exit 1 ;;
  3) echo "fsck could not run at all"; exit 1 ;;
esac
```

## See also

- `docs/refcount.md` -- the contract this tool reconciles: leakage allowed,
  loss never, one reference per block occurrence.
- `docs/adr/0005-fsck-scrub-reconciliation.md` -- why the tool looks like
  this: the closed-holder-set rule, the severity model, the degraded-flag
  decision and the alternatives that were rejected.
- `docs/adr/0006-block-write-protocol.md` -- the file-first write protocol
  whose residue classes these passes enumerate.
- `docs/adr/0002-blake3-hash-migration.md` -- addressing, store headers,
  and **verify-on-read**, the read-time complement to `--scrub`.
  Verify-on-read re-hashes blocks as they are served, so it only ever
  covers data someone reads, whole objects only (a range request cannot be
  re-hashed against a whole-block address). `--scrub` is the offline,
  whole-store half: it is the only thing that ever checks cold data. Run
  both.
- `docs/adr/0003-multipart-lifecycle-and-gc.md` -- where upload age and
  reaping will come from; until it lands the multipart pass reports and
  touches nothing.
