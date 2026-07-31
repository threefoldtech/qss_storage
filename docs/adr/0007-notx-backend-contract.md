# The fjall_notx Backend: Contract Scope and Fate

**Status**: Proposed
**Date**: 2026-07-31

---

## Context

`FjallStoreNotx` is the non-transactional metadata backend: a plain
`fjall::Database` behind the same `MetaStore` interface as the
transactional default. Its "transaction" is a pseudo-transaction --
writes land in the keyspace immediately, `commit` is a no-op `Ok(())`,
and rollback removes remembered inserts by hand; its own module doc
says "no isolation and no atomicity"
(`cas-storage/src/metastore/stores/fjall_notx.rs:119-127, 150-152`).
Its stated reason to exist: "Weaker guarantees than FjallStore, no
single-writer bottleneck" (`fjall_notx.rs:21-22`) -- it skips the
single-writer mutex that serializes every write transaction on the
default backend.

It is opt-in but fully productized: selectable via
`metadata_db = "fjall_notx"` in the config and `--metadata-db` on the
CLI ("default fjall" -- `s3cas/src/main.rs:75`), documented in
`qss_storage.toml.example:37`, handled by the offline tools
(`inspect.rs:75`, `check.rs:48`, `retrieve.rs:31`), and covered by a
startup warning in multi-user mode (`shared_block_store.rs:62-69`).

The ADR 0006 review (13-agent adversarial pass, 2026-07-30/31)
verified what the warning understates. On notx, today:

1. **Unserialized refcount RMWs are loss bugs, not "weaker
   consistency".** `write_block`'s get-then-insert bump loses updates
   under concurrent PUTs of one block (`meta_store.rs:627-651` over
   bare unlocked keyspace ops, `fjall_notx.rs:161-180`); the resulting
   undercount later frees a live block. The delete path double-counts
   the same way. Skeptic-verified with step-by-step interleavings.
2. **The Durability knob is dropped entirely.** `FjallStoreNotx::new`
   takes no durability parameter (`fjall_notx.rs:110-116`) and commit
   never persists: `durability = "fsync"` on a notx store fsyncs
   nothing at all -- neither metadata nor (today) block files.
3. **No atomic pair anywhere.** Even the minimal object-record
   read+remove that defeats concurrent double-DELETE of one key on the
   transactional backend has no notx equivalent.

ADR 0006 (accepted decisions, 2026-07-31) changes this picture
substantially but not completely:
- Its stripes serialize every block-record mutation per block, which
  incidentally fixes notx's PUT-vs-PUT and PUT-vs-DELETE rc races --
  the practical loss bugs above.
- Its file-first ordering plus durability-gated file fsyncs make block
  FILES durable on notx per the configured level, even though the
  metadata never is.
- What remains structurally unfixable without new machinery: concurrent
  DELETEs of one key (needs an atomic read+remove or a key-stripe), and
  metadata durability (needs persist wiring).

ADR 0006 therefore **scoped fjall_notx out of the loss-never
guarantee** (decided 2026-07-31): the contract of `docs/refcount.md` is
guaranteed on the transactional backend only. That decision is the
correct stance for 0006's scope -- but it leaves the backend itself
undecided: a production-selectable engine whose contract is "best
effort, undurable, mostly-fixed" is a standing footgun and a standing
cost (doubled test matrix, per-backend caveats in every doc, a warning
nobody reads). This ADR decides its fate.

Related: ADR 0006 (block write protocol; source of the scope-out and
the stripe fixes), ADR 0005 (fsck -- its passes must know which
backend invariants they may assume), `docs/refcount.md`.

---

## Decision

Proposed, pending review: **remove the fjall_notx backend.**

The argument is that ADR 0006 dissolves its reason to exist:

- **The bottleneck it avoids no longer bites.** Post-0006, every
  block-record mutation is serialized per block by a stripe and every
  fjall commit runs inside the stripe's `spawn_blocking` closure. The
  single-writer mutex the tx backend adds on top serializes only the
  *in-memory* transaction sections across distinct blocks --
  microsecond holds, with the journal persist outside the guard. No
  measurement has ever shown this as a limiting factor (the 0006
  review rejected fjall sharding on exactly that basis).
- **The speed knob it pretended to be already exists, safely.**
  `durability = "buffer"` on the transactional backend skips every
  fsync -- metadata and (post-0006) file alike -- while keeping
  run-time atomicity and isolation intact. That is the honest
  fast-and-loose tier: crash residue bounded and reconcilable by ADR
  0005, no run-time loss races. notx's only *additional* offer is
  trading run-time correctness for an unmeasured concurrency gain.
- **Carrying it is not free.** Every future invariant argument, every
  fsck pass, every stress test, and every doc paragraph must either
  branch per backend or carry an asterisk. The 0006 review spent a
  material fraction of its findings on notx-only interleavings; that
  tax recurs on every change to the write or delete path.

### Mechanics of removal

- Reject `metadata_db = "fjall_notx"` at config parse with an error
  naming the migration path ("use `fjall` with
  `durability = \"buffer\"` for the fast tier"). No silent fallback.
- Delete `fjall_notx.rs` and the `NonTransactional` flavor; keep the
  `fjall_common.rs` flavor abstraction only if it still carries its
  weight with one flavor (fold it back into `fjall.rs` if not).
- Remove the notx arms in `shared_block_store.rs`, `fs.rs`,
  `store_options.rs`, and the tools (`inspect.rs`, `check.rs`,
  `retrieve.rs`, `main.rs` CLI help).
- Halve the backend test matrix; delete notx-only tests.
- No data migration: no deployed store carries data (same stance as
  ADR 0006), and a notx store's on-disk format is a plain fjall
  database in any case -- were one to exist, opening it with the
  transactional backend is a re-import, not a conversion.
- `docs/refcount.md` loses its per-backend asterisk; ADR 0006's
  scope-out clause becomes historical.

---

## Alternatives Considered

### Keep it as an explicit best-effort tier
- **The idea**: make ADR 0006's scope-out permanent and honest --
  wire `db.persist()` into commit so Durability at least means
  something, rename the config value to something that reads as unsafe
  (`fjall_unsafe`), expand the startup warning into contract language,
  document the double-DELETE window.
- **Optimizes for**: keeping the maximum-throughput option available.
- **Sharpest tradeoff**: the permanent tax -- per-backend branches in
  every invariant argument, fsck pass, and test -- buys a tier whose
  advantage over `fjall` + `buffer` has never been measured, and whose
  users chose it (if ever) precisely because the name did not say
  "unsafe".
- **Bets on**: a real workload existing where the single-writer mutex,
  not the disk, is the limiting factor. Nobody has produced one. If
  one appears, this alternative can be revived from git with the
  hardening applied.

### Repair it to parity
- **The idea**: key-stripes for object deletes, persist wiring,
  keep-and-fix until its guarantees match the tx backend.
- **Sharpest tradeoff**: every fix converges its cost profile toward
  the transactional backend's (stripes and persists are where the time
  goes) while duplicating the machinery fjall's tx layer already
  provides. End state: two backends with the same guarantees and the
  same cost, one maintained by fjall upstream and one by us.
- **Bets on**: hand-rolled serialization staying correct through every
  future protocol change -- exactly the class of code the 0006 review
  kept finding bugs in.

### Do nothing (leave the 0006 scope-out as the final word)
- **The idea**: the contract asterisk stands; notx remains selectable.
- **Sharpest tradeoff**: a production config value that silently opts
  a store out of the project's headline guarantee, forever, guarded by
  one tracing::warn.
- **Bets on**: operators reading warnings. Rejected on its face.

---

## Consequences (of the proposed removal)

### Positive
- One backend, one contract: `docs/refcount.md`'s loss-never holds
  unconditionally; every invariant argument and fsck pass drops its
  per-backend branch.
- The test matrix halves; future write/delete-path changes need no
  notx-only race analysis.
- The fast tier gets an honest name: `durability = "buffer"` on the
  default backend.

### Negative
- Any workload that genuinely needed lock-free metadata writes loses
  its option -- mitigated by "measure first": the claim that such a
  workload exists has never been substantiated, and git preserves the
  backend if it ever is.
- Config break for any existing `metadata_db = "fjall_notx"` user:
  intentional, loud, with the migration path in the error message.

### Risks
- The `fjall_common.rs` flavor abstraction may not carry its weight
  with a single flavor; folding it back is mechanical but touches the
  whole metastore module. Assess during implementation rather than
  forcing either outcome now.

---

## Review asks

1. Remove (recommended), keep-as-hardened-tier, or repair-to-parity?
2. If removal: does anything in your deployment story select
   `fjall_notx` today? (In-repo, nothing does by default; the example
   config documents it but the default is `fjall`.)
3. Sequencing: removal is independent of ADR 0006's implementation
   order but simplifies it (plan components 5, 6, and 9 lose their
   notx arms) -- land the removal first?
