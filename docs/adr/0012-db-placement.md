# The Database Takes the Fast Disk: Tiered Store Roots

**Status**: Accepted, all four review asks APPROVED (owner, 2026-08-01):
automatic adoption at first open; recovery via fsck's explicit re-pair
verb only, no daemon override; paths stay CLI-only with the two roots as
the whole placement mechanism; no third `--blocks-db-path` knob until a
workload asks. Ready for implementation.
**Date**: 2026-08-01

---

## Context

The deployment this ADR exists for is spinning rust: bulk block data on
HDDs, where capacity is cheap, with the metadata database on an NVMe. The
store's IO profile makes the case by itself, with numbers from the ADR
0010 acceptance runs (2026-08-01):

- At fsync durability the store closes ~1300 commits per second under
  load, each a journal persist. On NVMe that was 92-95 percent utilization
  doing useful work; on a spinning disk every one of those persists is a
  seek away from the data stream it interrupts.
- Every dedup decision is a point read against the blocks tree, and every
  GET walks object records. Random reads on rust while sequential block
  data streams past them is the classic self-inflicted seek storm.
- Block DATA writes are the friendly half: 1 MiB files written whole into
  fanout directories -- as sequential as object storage gets. The data
  disk's workload is fine for rust; the database's is not.

The split half-exists already. `--fs-root` and `--meta-root` are separate
flags on every binary, and the paths flow separately: block data lands
under `<fs_root>/blocks/`, the namespace databases under `<meta_root>/`,
and the blocks database under `<meta_root>/blocks/.db`
(`CasFS::single_namespace` -> `SharedBlockStore::new(meta.join("blocks"),
fs.join("blocks"), ..)`). Run with the roots on different disks and the
database is already on the fast one.

What does NOT exist is anything that makes that a store shape rather than
an accident of two flags:

- **Nothing binds the pair.** The store header lives with the database
  (sidecar beside the DB dir); the blocks root carries no identity at all.
  Point an NVMe database at the WRONG data directory -- yesterday's, a
  sibling store's, an empty disk after a failed mount -- and the store
  opens cleanly and lies: records served whose files belong to someone
  else, dedup hits against blocks that are not there, new writes
  interleaved into a foreign fanout. fsck would eventually notice the
  carnage; the daemon at open does not.
- **Nothing verifies it.** The campaign, the harness rails, and the docs
  all assume one mount. The as-built docs do not mention the shape; the
  example config's path story is one root.
- In the common same-root deployment the database dir sits INSIDE the
  data fanout tree (the `blocks/.db` heritage, ADR "blocks/.db" commit
  2838f63), and scrub's foreign-file logic special-cases it there. Split
  roots change what scrub walks and what counts as foreign.

Related: the harness's tiered test-env extension (data-on-HDD,
meta-on-NVMe rails) is already on the owner's direction list; this ADR is
the product-side contract that extension will verify.

---

## Decision

Declare the two-root store a first-class, supported shape -- and make it
safe to be one. Three parts:

1. **Pairing identity.** Every store gets a `store_id` (UUID, minted at
   creation, recorded in the store header). The blocks root gets a small
   marker file, `blocks/.store-id`, holding the same id. At open, the two
   are compared; a mismatch is refused with both paths and both ids in the
   message and no override flag -- a mispaired store is not a warning, it
   is the wrong store. Existing stores (header without id, blocks root
   without marker) are ADOPTED on first open by the new build: id minted,
   written to both sides, one log line. Half-adopted states (crash between
   the two writes) resolve deterministically: the header side is
   authoritative, the marker is rewritten to match.

2. **Split-aware tooling.** fsck checks the pairing before anything else
   (a mispaired store must not be "repaired" toward either side's
   fiction); scrub's walk and foreign-file rules are stated for both
   layouts (`.store-id` and `.db` are the store's own, everywhere else in
   the fanout is block-shaped or foreign). The `.tmp` same-filesystem rule
   is unchanged and load-bearing: temp files and their renames stay on the
   DATA disk, so ADR 0006's rename atomicity never crosses a device.

3. **The shape is documented and verified, not invented per deployment.**
   The example config and as-built docs gain the tiered layout with the
   spinning-rust rationale; the harness's tiered rail (second mount)
   becomes the verification home for it. No new path knob is added: the
   two roots ARE the placement mechanism, they stay CLI-only by the
   existing design ruling ("paths are not config"), and the database
   follows the meta root as it already does.

What deliberately does NOT move: block-file fdatasyncs and the per-batch
fanout directory fsyncs. Those belong to the data and stay on the data
disk whatever it is; ADR 0010 already collapsed them to one wave per
request, and 0011 (proposed) merges waves across requests. On rust the
remaining data-side sync cost is the price of the contract, not a
placement mistake.

---

## Architecture Overview

### Component Breakdown

1. **Store identity** (`cas-storage/src/metastore/store_header.rs`)
   - `store_id: [u8; 16]` joins the header (header tree + sidecar, where
     the hash spec already lives). Minted at creation; adopted at first
     open when absent.

2. **Blocks-root marker** (`cas-storage/src/cas/block_disk.rs`, open
   duties)
   - `blocks/.store-id`: the id as hex, written via the existing
     temp+fsync+rename protocol, once. `AtomicBlockWriter::open` reads it
     and hands it up; `SharedBlockStore::new` compares against the header
     after the metastore opens, refuses on mismatch, adopts on absence.

3. **fsck pairing check** (`cas-storage/src/scrub`, `qss-storage-fsck`)
   - First check in every run: header id vs marker id, with a dedicated
     could-not-run-class refusal on mismatch. Scrub's foreign-file table
     adds `.store-id` beside `.db` as the store's own.

### Data Flow

```
NVMe (meta root)                  HDD (fs root)
  db/            namespace DBs      blocks/aa/../<hash>   block files
  blocks/.db/    blocks DB           blocks/.tmp/          staging (same fs)
  header+sidecar (store_id) <-----> blocks/.store-id      (must match)

journal persists, compactions,     1 MiB sequential writes, fdatasync
dedup point reads, record walks    waves, fanout dirsyncs, block reads
```

---

## Alternatives Considered

### Leave it undeclared (two flags, operator beware)
- **The idea**: the split already works; ship nothing.
- **Optimizes for**: zero work.
- **Sharpest tradeoff**: the mispairing failure is silent data corruption
  at open, on exactly the deployments (multi-disk, remounts, fstab edits)
  where wrong-path accidents are most likely.
- **Bets on**: operators never mispairing roots across reboots and disk
  swaps. Storage history says otherwise.

### One root plus symlinks
- **The idea**: keep a single `--root`; the operator symlinks
  `blocks/.db` (or `blocks/`) to the fast disk.
- **Optimizes for**: no flag surface at all.
- **Sharpest tradeoff**: the store cannot tell a deliberate symlink from a
  stale one; canonicalization already resolves links, so the pairing
  problem is identical but now invisible in the process args.
- **Bets on**: symlink hygiene being better than flag hygiene. It is the
  same hygiene with less visibility.

### A dedicated `--blocks-db-path` knob
- **The idea**: decouple the blocks DB from the meta root entirely; three
  placements.
- **Optimizes for**: maximum layout freedom (namespace DBs and blocks DB
  on different devices).
- **Sharpest tradeoff**: a third independently-wrong path multiplies the
  mispairing surface this ADR exists to close, for a split (metadata vs
  metadata) with no measured motive.
- **Bets on**: a workload where the two databases deserve different disks.
  None is known; the id mechanism here would extend to it if one appears.

### Paths in the config file
- **The idea**: `[store] fs_root/meta_root` in the toml for one-file
  deployment.
- **Optimizes for**: config ergonomics.
- **Sharpest tradeoff**: reverses the standing design ruling that paths
  are command-line only (the config describes the store, the invocation
  places it); a stale toml on the wrong host is the mispairing story with
  extra steps.
- **Bets on**: ergonomics outweighing the ruling. That is the owner's
  call, not this ADR's; the id check protects either way.

---

## Consequences

### Positive
- Spinning-rust deployments become supportable: the seek-heavy half of
  the store (journal, compactions, point reads) moves to NVMe with two
  flags, and the HDD keeps the sequential half it is good at.
- Mispairing goes from silent corruption to a refused open with both ids
  in the message -- also on today's single-root stores, where it catches a
  meta/fs flag typo just as well.
- The identity gives fsck a fact it never had: WHICH store a blocks tree
  claims to be, independent of what the database says.

### Negative
- A format addition: header gains a field, blocks root gains a dotfile.
  Old builds opening a new store's blocks root will see `.store-id` as a
  foreign file in scrub (harmless, reported, documented).
- Adoption writes to stores that never asked (one marker file, one header
  field, at first open) -- the price of not having a flag day.

### Risks
- A restored-from-backup data dir with a fresh database (or vice versa)
  now REFUSES to open where it used to limp. That is the feature, but it
  needs a documented recovery path: fsck gains the authoritative
  "re-pair" verb (explicit, takes both paths, rewrites the marker) so the
  escape hatch exists in the tool that can audit the result, not as a
  daemon flag that would get baked into unit files.
- Half-adopted crash states: header-then-marker write order with the
  header authoritative makes the repair deterministic; a crash fixture
  pins it.

---

## What an Expert Would Ask

**Q: NVMe database says the store has block X at depth 2; the HDD lost
power and its writeback with it. The pairing ids MATCH. What saves the
reader?**
A: Nothing new -- and nothing worse than today. Pairing identity is not
crash consistency: within one paired store, the durability contract is
ADR 0006/0010's, and at fsync the record only exists because its file was
durable first, on whatever disk. What the id closes is WRONG-STORE
pairing, not lost-write pairing. (A power-cut HDD behind an fsync store
is the existing degraded-record territory that scrub already grades.)

**Q: The database and the data now fail independently. What does losing
each look like?**
A: Lose the NVMe: records gone, data orphaned -- fatal today too (the DB
has no replica), just on a different disk; the fanout tree alone is not a
store. Lose the HDD: records dangle, scrub grades every one degraded --
identical to losing `blocks/` on a single root. The split changes WHICH
device takes the store down, not the blast radius; an operator who wants
DB redundancy wants it on either layout.

**Q: Does the marker survive fsck's own walks, GC, and `--fresh`-style
tooling that prides itself on refusing foreign files?**
A: The marker joins `.db` in the store's-own table everywhere that table
exists (scrub, the harness rail, the open-time temp purge). The harness's
`--fresh` recognizes it as campaign-created. That table is small and this
is its second-ever entry; a test asserts the walk skips exactly both.

**Q: On rust, the per-batch fanout dirsyncs stay on the HDD -- sixteen
directory fsyncs per 16 MiB part. Is that the next seek storm?**
A: Possibly, and it is measurable before it is tunable: the tiered
harness rail exists to produce that number. If it dominates, the known
mitigations are already shaped -- deeper batching (0011 merges dirsync
unions across requests), shallower fanout on rust, or the known-durable
ancestor cache absorbing repeats. Not solved here; named so the rail
measures it.

**Q: Why is adoption automatic instead of a migration refusal like
blocks/.db was?**
A: The blocks/.db refusal protected against SHADOWING existing records
with an empty database -- data loss on open. Adoption writes two small
identity artifacts and shadows nothing; refusing it would put a flag-day
in front of every existing deployment for zero protective value. The
asymmetry is deliberate and this paragraph is its record.

---

## Implementation Plan

### Decisions you will probably want to tweak
- **Id home**: header tree + sidecar (where the hash spec lives), marker
  as `blocks/.store-id` hex.
  - Alternative: a header sidecar in BOTH roots (symmetric). Cost to
    change later: small before ship, a format note after.
- **Mismatch refusal has no daemon override**; recovery is fsck's
  explicit re-pair verb.
  - Alternative: a `--force-pair` daemon flag. Cost to change later: none
    technically, but a daemon flag ends up in unit files and defeats the
    check; the fsck verb keeps a human in the loop.
- **Adoption on first open** (automatic, logged).
  - Alternative: explicit `fsck --adopt`. Cost to change later: none;
    flip only if automatic writes to old stores prove controversial.

### Known unknowns and how the plan absorbs them
- **Tiered performance on actual rust**: the projected win (journal +
  point reads off the HDD) is argued, not measured -- no spinning-rust rig
  exists yet. Default: ship the safety, let the tiered harness rail (its
  own planned work) produce the numbers. Signal to revisit: the rail
  showing the dirsync tail dominating -> the mitigations named above, in
  measured order.
- **Old-build-meets-new-store friction**: `.store-id` reported foreign by
  old scrub. Default: document. Signal: real deployments mixing builds ->
  backport the skip-table entry.

### The mechanical work
Header field + sidecar plumbing (`store_header.rs`); marker write/read in
`AtomicBlockWriter::open`; the compare-refuse-adopt in
`SharedBlockStore::new`; fsck pairing check + re-pair verb; scrub
foreign-table entry; example-toml and as-built docs sections; tests:
mismatch refusal (both directions), adoption, half-adoption crash
fixture, scrub-skips-marker, and the harness tiered rail asserting the
whole shape end to end.

### Review asks (all ruled, owner, 2026-08-01)
1. Automatic adoption at first open (vs explicit fsck verb): APPROVED.
2. Recovery via fsck re-pair only, no daemon override flag: APPROVED.
3. Keep paths CLI-only (no toml paths), the two roots as the whole
   placement mechanism: APPROVED.
4. Scope: no `--blocks-db-path` third placement until a workload asks:
   APPROVED.

---

## Open Questions

**Architecture-changers**
- [ ] Should the store_id also gate the RESP store's `--data-dir` (its
      own DB, same mispairing exposure)? Proposed: yes, same mechanism,
      separate small change -- respd stores no blocks, so it is marker-in-
      sidecar only.

**Behavior definers**
- [ ] Multi-namespace stores: namespace DBs live under the meta root
      beside the blocks DB -- does each need its own pairing, or does the
      store-level id cover them? Proposed: store-level covers them; they
      are opened through the same paired root.

**Polish**
- Marker filename `.store-id`: proposed as written.
