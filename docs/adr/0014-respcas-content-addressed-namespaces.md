# respcas Content-Addressed Namespaces

**Status**: Proposed
**Date**: 2026-08-04
**Updated**: 2026-08-04 (all four review asks ruled by owner; verification
semantics reworked per ruling 4 -- dedup hits are served by reference,
without re-verification)

---

## Context

respcas is named for content-addressed storage and does none. Every value is
stored under a client-chosen key as `ObjectData::Inline`, unconditionally
(`respcas/src/namespace.rs:244`); `get` errors on any non-inline record
(`namespace.rs:268`); the MD5 it computes on SET is a checksum filed inside
the record (`CHECK` re-hashes against it, `namespace.rs:331`), addressing
nothing. The rename commit (7d812da) claimed "a CAS store wearing a Redis
face"; the storage under the face is a plain KV metastore.

Meanwhile the workspace owns a real CAS engine that respcas does not use:
the ADR 0006 block write protocol (1 MiB blocks, `fs.rs:17`), BLAKE3 block
addressing (ADR 0002), the rc lifecycle with release-on-displace (ADR 0008),
fsck walkers (ADR 0005), the ack durability contract (ADR 0013), and store
pairing identity (ADR 0012). s3cas exercises all of it; respcas none.

Two client workflows motivate the change:

- **Server-hashed put**: "store this, tell me its address." The client does
  not know the hash; the server computes it and returns it as the key.
- **Client-hashed put with probe**: the client already has the hash (it
  hashed the content itself, or received the hash from elsewhere). It asks
  `EXISTS <hash>` and transfers the content only on a miss. This is the
  dedup-upload pattern (git, restic, casync): the win is skipped transfer.

The seam for per-namespace behavior already exists: `NamespaceMeta.key_mode`
with `KeyMode { UserKey, Sequential }` (`respcas/src/storage.rs:185-192`),
zdb heritage; `Sequential` is currently vestigial (referenced only in the
NSINFO display string, `cmd.rs:632`).

ADR 0004 (one process owns a store) is untouched: respcas keeps its own
exclusive store. This ADR changes what respcas stores in it.

---

## Decision

Add a third key mode, **`KeyMode::Cas`**, selectable per namespace. In a
Cas namespace:

1. **The key of every record IS the BLAKE3-256 hash of its value** -- 32
   raw bytes on the wire (RESP is binary-safe; inspect tooling prints hex).
2. **Ingest has two modes, one storage path** (ruled: both wire forms for
   mode A, so existing zdb-style clients keep working and new clients get
   an explicit verb):
   - Mode A, server-hashed: `SET "" <value>` (empty key sentinel, the zdb
     sequential-mode wire shape) **and** `CSET <value>` are equivalent --
     the server hashes the value, stores it, and replies with the 32-byte
     key as a bulk string.
   - Mode B, client-hashed: `SET <32-byte-key> <value>` -- the server
     stores content under the claimed address per the trust rule below.
     A key of any other length is an error.
3. **Verification protects the first write of an address; presence
   substitutes for it afterwards** (ruling 4). The invariant is that bytes
   are verified against their address exactly once: on the write that
   first materializes them. Concretely:
   - Mode B, address `H` not present in any Cas namespace: verify
     `blake3(value) == H`; mismatch is an error and nothing is written;
     match proceeds to the write path.
   - Mode B, `H` already present in this namespace: the record exists and
     its content was verified on first ingest -- ack, discard the incoming
     bytes unverified, write nothing, rc unchanged.
   - Mode B, `H` present in another Cas namespace: the content is implied
     already there -- create this namespace's record **by reference** from
     the existing one (block-backed: same block list, rc+1 per block;
     inline: copy the stored, already-verified bytes), discard the
     incoming bytes unverified, skip the write path entirely. Every
     namespace referencing `H` holds its own rc contribution, so a later
     DEL in one namespace never strands another.
   - Mode A: hashing the value IS the verification (the key is derived,
     it cannot mismatch); after hashing, the same presence checks apply
     and the write is omitted on any hit.
   Reference sources are **Cas namespaces only**: a 32-byte user key in a
   UserKey namespace is a coincidence, not a verified address, and is
   never consulted.
4. **`EXISTS` is the probe.** Unchanged semantics, namespace-scoped;
   clients pipeline one EXISTS per hash. A batch probe (`MHAS`) and a
   store-wide probe are committed follow-ups, not v1 (see Deferred).
5. **Values route through the block engine.** At or below the inline
   threshold (`max_inlined_data_length`) the record stays `Inline` exactly
   as today; above it, the value is chunked through the ADR 0006 write path
   into a SharedBlockStore and the record is `SinglePart { blocks }`. The
   whole-value hash is the *tree key*; it is never a `BlockId`
   (`block.rs:22`: "a BlockId is never a content hash of a whole object" --
   that doctrine holds; the two address spaces stay disjoint by role).
6. **UserKey namespaces are unchanged bit-for-bit.** No existing behavior,
   record, or wire reply moves.

Overwrite semantics fall out of the model: `SET` of an existing key with
matching content is an idempotent success (dedup hit); with mismatching
content it is a verification error. A Cas namespace is therefore immutable
per key by construction -- `DEL` is the only real mutation.

---

## Architecture Overview

### Component Breakdown

1. **`KeyMode::Cas`** (`respcas/src/storage.rs`)
   - Third enum variant, msgpack-serialized inside `NamespaceMeta`.
   - Set via `NSSET <ns> key_mode cas`, **permitted only while the
     namespace is empty** (DBSIZE == 0). Switching a populated namespace is
     incoherent (existing keys would stop meaning what they say) and is
     refused with a clear error. `NSINFO` reports `key-mode: cas`.
   - Compatibility gate: a store whose meta contains the new variant must
     not be opened by an older respcas that would fail msgpack decoding
     mid-flight. The QSST header minor version is bumped when the first Cas
     namespace is created; older builds refuse the open with the standard
     header error. This is exactly what the header exists for (ADR 0002).

2. **Cas-mode command semantics** (`respcas/src/cmd.rs`, `namespace.rs`)
   - `SET` dispatches on namespace mode: empty-key form (and its alias
     `CSET`, a new parsed command valid only in Cas namespaces) returns
     the computed key; keyed form runs presence-then-verify-then-store per
     Decision 3. Reply shapes in UserKey namespaces are untouched.
   - Clone-by-reference is a new metastore operation: insert a record into
     one namespace tree copying an existing Cas record's data, bumping
     each referenced block's rc in the same transaction (the ADR 0003/0008
     refcount machinery; no block bytes move). Its source is restricted to
     Cas-mode namespaces by construction.
   - `GET <key>`: unchanged surface; gains the block-backed read path (see
     component 4). `DEL`, `LENGTH`, `KEYTIME`, `SCAN`/`RSCAN` unchanged
     (SCAN enumerates hash keys in tree order, which is hash order).
   - `CHECK <key>`: extended to block-backed records -- streams the blocks
     and re-hashes the whole value against the key (in Cas namespaces the
     key is the stronger digest; the stored MD5 remains as the envelope
     field it already is). Cost is O(size), same class as GET.
   - WORM composes: `cas + worm` is a true immutable archive -- DEL is
     refused, so an EXISTS answer can never be invalidated (see Risks).

3. **Block-backed storage in respcas** (`respcas/src/storage.rs` +
   `cas-storage`)
   - `Storage` grows a `SharedBlockStore` (the 9-argument constructor from
     ADR 0011/0012), mirroring the s3cas layout: the data dir becomes
     `{store_header.bin, db/, blocks/}`. `blocks/` is created on first open
     of a build that supports Cas mode; existing stores gain it additively.
     ADR 0012 store-id pairing between meta and blocks applies as built.
   - Writes reuse the existing engine verbatim: a respcas namespace is
     already a bucket to the metastore (`create_namespace` goes through
     `insert_bucket`, `storage.rs:136`), so the block write path is called
     with bucket = namespace name. No new write protocol; ADR 0006/0008/
     0010/0013 semantics apply unchanged, including the ack durability
     classes.

4. **Block-backed GET** (`respcas/src/namespace.rs`)
   - Non-inline records stream their blocks and concatenate. The
     `"Object is not inline"` error path disappears for Cas namespaces
     (and only there; UserKey namespaces still never produce non-inline
     records).
   - RESP bulk replies are length-prefixed; the record's stored size field
     supplies the length before streaming begins.

5. **Limits** (`respcas` config)
   - `max_value_size` knob (proposed default 64 MiB). RESP command parsing
     buffers the full value in memory before the write path runs; the knob
     bounds that honestly rather than pretending the daemon streams ingest.
     zdb's default payload cap is 8 MiB; respcas serves broader workloads,
     hence the larger default.

### Data Flow

```
Mode A (server hashes)                Mode B (client hashed)

client            respcas             client              respcas
  |-- SET "" v ----->|                  |-- EXISTS H ------->|
  |   (or CSET v)    | H = blake3(v)    |<-- :0 -------------|   (miss)
  |                  | presence(H)?     |-- SET H v -------->|
  |                  | miss: store      |                    | presence(H)?
  |<-- $32 H --------|                  |                    | miss: verify
                                        |                    |   blake3(v)==H,
                                        |                    |   then store
                                        |<-- +OK ------------|
                                        (on :1 hit: no transfer at all)

presence(H), checked before any verify or write:
  this namespace has H        -> ack; discard bytes; nothing changes
  another Cas namespace has H -> clone record by reference:
                                   block-backed: same block list, rc+1 each
                                   inline: copy stored (verified) bytes
                                 discard incoming bytes; no write path
  nowhere                     -> mode B verifies, then store v under H

store v under H:
  len(v) <= inline threshold  -> record H -> Inline{v}
  else                        -> ADR 0006 write path: 1 MiB blocks,
                                 BlockIds, rc, blocks/ files
                                 record H -> SinglePart{blocks}
                                 (block-level dedup still applies to
                                  chunks shared with unrelated content)
```

`presence(H)` is a point lookup in this namespace's tree, then one point
lookup per other Cas-mode namespace (enumerated from `_BUCKETS` metadata,
`key_mode == Cas` only). Fjall point reads make this O(#cas-namespaces)
cheap reads; a store-wide content index is deliberately NOT introduced in
v1 -- it would be a second reference-holding structure with its own
lifecycle. Revisit only if cas-namespace counts grow large.

---

## Alternatives Considered

### Per-command CAS (new CSET/CGET commands, no namespace mode)
- **The idea**: leave namespaces alone; add commands that store by hash
  alongside normal SET in the same namespace.
- **Optimizes for**: zero namespace-metadata changes.
- **Sharpest tradeoff**: two addressing regimes interleaved in one tree --
  SCAN returns a mix of user keys and hashes nothing can tell apart, and
  every key-shaped invariant (32 bytes means hash) evaporates.
- **Bets on**: clients wanting to mix addressed and named data in one
  namespace. Observed clients want one or the other per dataset.

### Inline-only CAS (hash-keyed records, no block engine)
- **The idea**: keep today's storage exactly; just make the key the hash.
- **Optimizes for**: smallest possible diff (no blocks/, no rc).
- **Sharpest tradeoff**: large values keep riding the fjall journal whole
  -- a 1 GiB SET is a 1 GiB WAL entry -- and no storage is shared or
  deduplicated below the whole-value level.
- **Bets on**: values staying small forever. The probe-before-upload
  workflow exists precisely because values are big.

### Expose the block store directly (keys are BlockIds)
- **The idea**: skip the object layer; let clients read and write 1 MiB
  blocks by BlockId.
- **Optimizes for**: zero mapping between wire and storage.
- **Sharpest tradeoff**: violates the standing doctrine that BlockIds are
  internal (`block.rs:22`) and welds the wire protocol to the chunk size;
  changing chunking ever after breaks every client.
- **Bets on**: chunking never changing. ADR 0006 changed it once already.

### Hex-encoded keys on the wire
- **The idea**: 64-char lowercase hex instead of 32 raw bytes.
- **Optimizes for**: human-readable debugging.
- **Sharpest tradeoff**: doubles every key on the wire and in client
  memory for a protocol that is binary-safe end to end; zdb precedent is
  raw bytes.
- **Bets on**: humans reading wire traffic being more common than programs
  moving bulk data. Inspect tooling printing hex covers the humans.

---

## Consequences

### Positive
- The daemon's name becomes true; the s3cas/respcas split becomes "same
  engine, two faces", which is what the workspace claims it is.
- Dedup: identical content stored once per store, across namespaces
  (blocks are store-wide; rc counts all referrers).
- Large values stop transiting the metadata WAL; the fjall journal carries
  ~40-byte records again regardless of value size.
- fsck coverage: Cas-namespace data lands in the trees and block store the
  ADR 0005 walkers already scrub.
- `cas + worm` yields an immutable content archive with a hard EXISTS
  guarantee.
- Replication groundwork: a Cas namespace is the trivially-replicable
  class -- concurrent SETs of one key carry identical bytes by
  construction, so convergence needs no register semantics; only DEL needs
  tombstones. The eventual replication ADR inherits a much smaller
  problem for this data class.

### Negative
- respcas inherits the rc lifecycle and its complexity (release on DEL,
  the concurrent-write staging protocol) where today it has none.
- Store layout change (blocks/ appears) and a QSST header minor bump; old
  binaries refuse stores that have used Cas mode.
- The RESP value buffer is now the memory high-water mark for ingest;
  bounded by `max_value_size` rather than eliminated.

### Risks
- **Probe-then-DEL race**: client sees `EXISTS H -> 1`, skips upload;
  another client DELs H before the first references it. EXISTS is
  documented as advisory in non-worm Cas namespaces; the hard guarantee is
  `cas + worm`. Mitigation is documentation plus the worm pairing, not
  mechanism.
- **Concurrent same-key SETs** land in the ADR 0006 staging protocol
  (dedup hits retain bytes until batch commit; losing stage discarded).
  The machinery is campaign-tested via s3cas; respcas adds tests, not
  protocol.
- **Clone-vs-DEL race**: a cross-namespace clone reads a source record
  that a concurrent DEL is releasing. The clone op runs record-insert plus
  rc+1 in one transaction on the single-writer store, so it serializes
  against the DEL's decrement -- the same one-tx guarantee class as ADR
  0003's `claim_upload_with_parts`. The clone either lands before the DEL
  (rc never reaches zero) or after it (presence(H) misses and the write
  path runs). Pinned by a dedicated test, not left to inference.
- **msgpack enum evolution**: adding a variant must not silently corrupt
  older metas. Round-trip tests pin old-meta compatibility; the header
  bump gates the other direction.

---

## What an Expert Would Ask

**Q: Can a malicious client poison an address in mode B?**
A: No. Bytes only ever materialize under an address through a verified
write: a mode-B miss verifies before storing, and mode A derives the key
from the bytes. On a presence hit the incoming bytes are *discarded* --
the record is served from content that passed verification when it was
first written, so `GET H` can only ever return bytes that hash to H. The
deliberate casualty of ruling 4: a buggy client that sends wrong bytes
under an already-present H gets `+OK` instead of an error, because nobody
hashes what will not be kept. The store is unpoisonable either way; the
diagnostic courtesy is traded for not hashing discarded data.

**Q: EXISTS said 1, so the client skipped the upload -- what if the content
is gone when it matters?**
A: Not handled mechanically in non-worm namespaces, and that is accepted:
the probe is advisory, identical to git's have/want against a repo that can
be force-pruned concurrently. Deployments that need the guarantee use
`cas + worm`, where DEL is refused and an EXISTS answer is permanent.

**Q: Why is the key the whole-value hash rather than a manifest/merkle root
of the block hashes?**
A: So a client can compute keys with stock tooling (`b3sum`) and no
knowledge of our chunking. A merkle-root key would tie every client to the
1 MiB block size and to any future change of it. Cost: the server hashes
the whole value on ingest -- it is already reading every byte, and BLAKE3
runs at memory bandwidth.

**Q: What does CHECK mean now that the strong hash is the key?**
A: In Cas namespaces CHECK re-hashes the full value (streaming blocks if
non-inline) against the key -- a strictly stronger check than the MD5
comparison it performs in UserKey namespaces. The stored MD5 field stays
because the shared envelope has it; it carries no authority in Cas mode.

**Q: A 64 MiB SET buffers 64 MiB in the RESP parser. Is that acceptable?**
A: Yes, and it is stated rather than hidden: ingest is buffer-then-write,
bounded by `max_value_size` times concurrent connections. Streaming RESP
ingest would require chunked-transfer framing the protocol does not have.
Clients with larger objects belong on the S3 face, which has multipart.

**Q: What happens to `inspect`/fsck/dump tooling that assumes respcas
stores are metadata-only?**
A: The store stops being special: it becomes a standard meta+blocks pair,
which is the layout every existing walker, scrubber, and the ADR 0012
pairing check were built for. The acceptance run must include fsck over a
store with both namespace kinds populated.

---

## Implementation Plan

### Decisions you will probably want to tweak

- **Mode-A wire: both `SET "" <value>` and `CSET <value>`** (ruled: both,
  so zdb-shaped client calls keep working and new clients get an explicit
  verb).
  - Cost to change later: client-visible wire shape; dropping either form
    after external clients ship means a compatibility shim forever.
- **Key encoding: 32 raw bytes.**
  - Alternative: 64-byte hex.
  - Cost to change later: wire break for every stored key; effectively
    permanent once data exists.
- **Mode selection: `NSSET key_mode cas`, empty namespace only.**
  - Alternative: an NSNEW argument (`NSNEW <name> cas`).
  - Cost to change later: low; additive surface either way.
- **`max_value_size` default: 64 MiB.**
  - Alternative: zdb's 8 MiB, or unbounded.
  - Cost to change later: a config default; trivial. Signal to revisit:
    clients hitting the cap in practice.

### Known unknowns and how the plan absorbs them

- **msgpack variant compatibility**: default assumption is that adding the
  variant round-trips old metas untouched. Pinned by tests that decode
  metas serialized by the current build. Signal to pivot: any decode
  asymmetry -> an explicit meta migration on open, gated by the same
  header bump.
- **Block-path GET latency vs today's inline reads**: assumption is parity
  for small values (unchanged path) and acceptable streaming for large.
  Signal: the realtest harness's respcas rig regressing on small-value
  latency -> inline threshold tuning, not redesign.

### The mechanical work

- `KeyMode::Cas` variant; NSSET gate (empty-only) + error; NSINFO string;
  header minor bump on first Cas namespace; old-meta round-trip tests.
- SET dispatch (sentinel + `CSET` + keyed paths), presence(H) lookup over
  Cas namespaces, clone-by-reference metastore op (record copy + rc+1 per
  block, one tx), verify-on-miss, GET/CHECK block-backed reads, DEL
  through rc decrement.
- `Storage` construction over `SharedBlockStore` (mirror the s3cas
  9-arg call); config plumbing: inline threshold shared, `max_value_size`.
- Tests: mode A/B round trips, verify-reject on miss, presence-hit
  discard (same-ns no-op; cross-ns clone with rc counts asserted),
  32-byte UserKey key never used as clone source, cas+worm refusals,
  concurrent same-key SETs, DEL in one namespace leaving another's clone
  readable, old-store open (no blocks/ dir), fsck walk over mixed-mode
  store.
- Docs: respcas README command table, EXISTS-is-advisory note.

### Deferred (committed -- not v1, must not be forgotten)

- **`MHAS h1 h2 ...`**: batch probe returning an array of 0/1, so a
  many-chunk client resolves its miss-set in one round trip instead of a
  pipeline of EXISTS.
- **Store-wide probe / bytes-less link**: EXISTS is namespace-scoped, so
  a client cannot discover that content exists in a *different* namespace
  and skip the transfer; today that transfer is absorbed server-side by
  presence(H) (bytes discarded, nothing written). A store-wide probe or an
  explicit "link H into this namespace" command would save the wire hop
  too. Rides with MHAS.
- Tracked as a vestige intention against this ADR so it resurfaces when
  implementation starts.

### Review asks -- ruled (owner, 2026-08-04)

1. Mode-A sentinel: **both** `SET ""` and `CSET`, so existing zdb-shaped
   client calls keep working.
2. Batch probe: **deferred, committed** -- see Deferred above.
3. `key_mode` via NSSET, empty namespace only: **yes**.
4. Dedup-hit verification: **no re-verification; serve by reference** --
   keyed mode implies the data is already there (clone the record, rc+1
   per block per namespace, drop the value write); return-key mode's
   hashing is inherent verification, and the write is omitted on any hit.
   Folded into Decision 3.

---

## Open Questions

**Architecture-changers**
- [ ] Should UserKey namespaces eventually gain the block path for large
      values too (removing the inline-only ceiling everywhere)? Out of
      scope here; if yes later, it is its own small ADR because it changes
      UserKey durability/latency behavior with no wire change.

**Behavior definers**
- [ ] `DEL` in non-worm Cas namespaces: default-allowed (this ADR) or
      config-refusable per namespace short of full worm?
- [ ] `max_value_size` default (64 MiB proposed above).

**Polish**
- NSINFO mode string `cas`; hex casing in inspect output. Defaults
  proposed, not worth debate.
