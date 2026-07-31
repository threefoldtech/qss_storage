# ADR 0008 Implementation Plan: overwrite releases the replaced blocks

Executes `docs/adr/0008-overwrite-releases-replaced-blocks.md` (Accepted
2026-07-31, all three review asks approved, self-copy short-circuit
chosen). Verified against `development` at 4506f32; the file:line facts
below come from the caller audit in component 0, which ran first and is
recorded here rather than thrown away.

The ADR is the authority on *why*. This is the component-level *what*,
written to be executed in a fresh session with no other context.

## Decisions already made -- do not reopen (rationale in the ADR)

- **Both write paths.** `create_object_meta` AND `store_inlined_object`
  release what they displace. An inline write over a block-backed object
  is the interesting case: the new record holds no references at all, so
  without the release every one of the old object's occurrences leaks.
- **One-transaction read-and-replace.** A `replace_object` sibling of
  `take_object` on the namespace-DB transaction, returning the displaced
  `Option<Object>`. Concurrent overwrites of one key serialize under
  fjall's single-writer tx; each writer releases exactly the record it
  displaced, so no record is released twice and none is missed.
- **Ordering is load-bearing.** The new record COMMITS FIRST; the old
  record's blocks are released after. A crash between the two leaks
  (INFO, fsck's recount collects), never loses. Never release before the
  replace, never inside it: block records live in the shared DB and
  object records in the namespace DB, no transaction spans them, and rc
  mutations happen under stripes which are never held inside a tx (the
  ADR's third rejected alternative).
- **Synchronous release** in the write path, reusing `release_blocks`
  (ADR 0003 component 3) unchanged: striped, per occurrence, never
  aborting. No deferred queue until measured.
- **Self-copy short-circuits** as a no-op: detected up front, success
  returned, no record rewrite, no refcount touch. See component 5 for
  what that means at this HEAD.

## Hard rules (apply to every component)

1. **New record first, release second.** Stated at the release site in a
   comment, in the same terms ADR 0003's abort loop uses. Every failure
   between the two must land in the leak direction.
2. **The replace is one transaction.** Read-then-blind-write is the bug
   this replaces: two overwriters would both read the same displaced
   record and both release it, which is a double decrement -- loss.
3. **No transaction is live across an await.** The replace tx begins and
   commits inside one synchronous helper; `release_blocks` runs after
   the helper returned. This preserves the `Send`-soundness rule on
   `FjallTransaction` and hard rule 6 of ADR 0006 (fjall is the leaf
   lock: never acquire a stripe while holding the fjall guard).
4. **`release_blocks` is not reimplemented.** Object delete, multipart
   abort, the GC and now overwrite are one operation over an explicit
   block list. There is exactly one place references are dropped.
5. Plain ASCII. Gates per commit: `cargo fmt --all`, `cargo check
   --all-targets`, `cargo clippy --all-targets`, `cargo test
   --workspace`. One commit per component, subject naming the component.

---

## Component 0: caller audit (ran first, no behavior change)

The ADR's one named unknown: whether any caller depends on overwrite NOT
touching the old blocks. Every call site of both write paths, at
4506f32:

**`create_object_meta`**

| Call site | Context | Overwrite possible? | Depends on the leak? |
| --- | --- | --- | --- |
| `cas/write_path.rs:275` (`store_single_object_and_meta`) | async, the PUT path | yes -- the routine same-key re-PUT | no |
| `s3cas/src/s3fs.rs:311` (`complete_multipart_upload`) | async | yes -- complete over an existing key | no. The object's own blocks come from the claimed part records, which the claim transaction removed; the displaced record is a different object entirely. Shared blocks are exact by the dedup arithmetic (each part's write bumped). |
| `cas/gc.rs:596` | `#[tokio::test]`, hand-rolled complete | no (fresh key) | no |
| `scrub/tests.rs:67` (`plant_object`) | fixture, called from `#[tokio::test]`s | no (fresh keys in every caller) | no. The fixture plants a record naming blocks that were never written; it never displaces one. |

**`store_inlined_object`** (which delegates to `create_object_meta`, so
it inherits whatever that does)

| Call site | Context | Overwrite possible? | Depends on the leak? |
| --- | --- | --- | --- |
| `s3cas/src/s3fs.rs:977` (`put_object`, small body) | async | yes -- including inline over a block-backed object, the case the ADR calls out | no |
| `s3cas/src/inspect.rs:278` (test helper) | sync `#[test]` helper | no (distinct keys) | no |
| `benches/casfs_benchmark.rs:93,143,170` | criterion, sync | no (random keys) | no |
| `cas/fs.rs:1001`, `scrub/tests.rs:125` | tests | no | no |

**Not a caller: `copy_object`.** `s3cas/src/s3fs.rs:337` answers
`NotImplemented`. There is no destination write and no CasFS-level copy
either. See component 5.

**Not a caller: respd's `set`.** The ADR describes it as covered by the
same code path; at this HEAD it is not. `respd/src/namespace.rs:219-247`
builds an `Object` with `ObjectData::Inline` and writes it straight
through `MetaTreeExt::insert` on the namespace tree. respd has no
`CasFS`, no block store and no refcounts, so there is nothing to
release and no path to release it on. The ADR's *conclusion* holds --
no-op in practice -- for a structural reason rather than a shared-code
reason. Nothing to do; recorded so the next reader does not go looking.

**Audit result: nothing in tree depends on overwrite leaking.** Two
things depend on the *arithmetic* being what ADR 0006 made it, and both
stay correct: the dedup bump-always rule (shared blocks net to unchanged
rc) and fsck's recount (it walks truth regardless).

## Component 1: `Transaction::replace_object`

`cas-storage/src/metastore/meta_store.rs`, beside `take_object`
(`:608`), same shape and same doc register:

```rust
pub fn replace_object(
    &mut self,
    bucket: &str,
    key: &str,
    raw_obj: Vec<u8>,
) -> Result<Option<Object>, MetaError>
```

Reads the record at `key`, writes `raw_obj` over it, returns what it
displaced (`None` on a fresh key). Read and write are one transaction:
that is what makes the returned record *this* writer's to release.

Decoding the displaced record can fail. It must fail the whole call --
never write over a record whose block list could not be read, because
the references it names would be stranded with no holder that can ever
name them again. The caller rolls back.

Acceptance: unit tests in the existing `meta_store` test module -- a
fresh key returns `None` and the record lands; an occupied key returns
the old object and the new record is the one readable after; two
sequential replaces each displace exactly the previous record; a
rolled-back replace leaves the original record intact.

## Component 2: wire into `create_object_meta`

`CasFS::create_object_meta` (`cas/fs.rs:267`) moves its body into
`cas/write_path.rs` (the house pattern -- `fs.rs` delegates) and becomes
`async`:

```text
obj  = Object::new(size, hash, object_data)
old  = one namespace tx { replace_object(bucket, key, obj.to_vec()); commit }
if old holds blocks:
    release_blocks(shared, metrics, old.blocks())      # AFTER the commit
```

The transaction lives inside a synchronous helper so no transaction is
ever held across the await (hard rule 3). Errors roll back and return;
the release runs only after a successful commit.

`async` propagates to every caller from the audit table. All of them are
already async or already inside a runtime except three:

- `benches/casfs_benchmark.rs` -- `bench_inlined_object_sizes` gains the
  `Runtime` its two sibling benchmarks already build; all three inline
  call sites go through `rt.block_on`.
- `s3cas/src/inspect.rs`'s `store_with_keys` test helper -- builds a
  current-thread runtime and blocks on the writes.
- `cas/fs.rs`'s `do_test_store_inlined_object` -- becomes `async`, its
  `#[tokio::test]` caller awaits it.

Acceptance: an overwrite of a block-backed object by another
block-backed object leaves rc at exactly the new object's occurrences;
distinct content drops the old block to zero (record gone, file
unlinked); same content leaves rc exactly 1 (bump then release).

## Component 3: `store_inlined_object`

`store_inlined_object` (`cas/write_path.rs:286`) delegates to
`create_object_meta`, so it inherits component 2 with no code change
beyond `async`. What it does NOT inherit is a test: the case the ADR
names -- an inline write replacing a block-backed object -- is the one
where the new record holds no references at all, so the release is the
only thing standing between an overwrite and a permanent leak.

This component is the doc comment stating that, plus the tests:

- inline over block-backed: the old object's blocks reach rc 0, record
  gone, file unlinked;
- inline over inline: nothing to release, no stripe taken, the record is
  the new one (the respd-shaped case, exercised through `CasFS` since
  respd itself does not use this path -- see component 0);
- block-backed over inline: the inline record holds no references, so
  the release is a no-op and the new object's blocks are at 1.

## Component 4: the rc-exactness allowances die

The 0006-series tests carry the leak as an asserted expectation. Every
one of them must now assert exactness.

- `cas/fs.rs`'s `do_test_store_and_delete_object_with_refcount_same_blocks_samekey`
  (`:1244-1307`): the comment block explaining that the overwrite
  "does not decrement the replaced object's blocks", the
  `assert_eq!(block.rc(), 2, "every dedup hit bumps")` after the
  re-PUT, and the `assert_eq!(block.rc(), 1, "leak-class residue,
  never loss")` after the DELETE. Post-0008 the re-PUT lands at rc 1
  (bump to 2, release back to 1) and the DELETE takes it to zero:
  record gone, file unlinked. Rename the test to say what it now pins.
- Code comments that name the same-key overwrite as a *routine* leak
  producer: `cas/crash_fixtures.rs:95-99` (`plant_inflated_rc`),
  `scrub/passes.rs:30` and `:77`, `scrub/findings.rs:43`. The residue
  class stays real -- a crash between the replace commit and the
  release produces exactly it -- so the fixtures and findings do not
  change; only the sentence naming successful overwrite as a source.

Acceptance: the whole suite green with every rc assertion tightened; no
`grep -i "overwrite.*leak"` hit left in `cas-storage/src` describing
current behavior.

## Component 5: self-copy short-circuit -- BLOCKED at this HEAD

The ADR's answered open question requires `copy_object` with source ==
destination to short-circuit. `s3cas/src/s3fs.rs:337` returns
`NotImplemented` for every copy, and no CasFS-level copy exists, so
there is no destination write to guard and no rc to pin a test against.

Implementing the guard alone would make copy-onto-self succeed while
every other copy answers 501 -- an S3 surface nobody specified, and one
a client would reasonably read as "copy works". Implementing
`copy_object` is a different piece of work with no ADR.

Left undone deliberately. The decision is recorded and binds whoever
implements `copy_object`: the self-copy check is the first thing in the
handler, before any record write. Reopen with the owner.

## Component 6: race arms

`cas/race_tests.rs`, in the register of the existing arms (written
against invariants, not implementation):

- **Overwrite storm on one key.** K tasks overwrite one key with K
  distinct contents, repeatedly. At quiesce exactly one object survives
  and rc is exactly its occurrences -- every displaced record was
  released by the writer that displaced it, exactly once. Both the
  distinct-content shape (each overwrite drops a block to zero) and the
  same-content shape (bump and release net to nothing, rc pinned at 1).
- **Overwrite vs delete of one key.** Both interleavings release each
  record exactly once. At quiesce either the key is gone and rc is zero,
  or the key holds the overwriting object and rc is exactly its
  occurrences. Nothing in between.
- **Overwrite vs a reader of the old object.** The equivalence the ADR
  demands be stated and verified: a reader that resolved its block list
  from the record an overwrite then displaced is in EXACTLY the
  delete-vs-reader race, which already exists and is already accepted
  (an open stream survives the unlink by POSIX fd semantics; an
  open-after-unlink fails loudly). The test opens the old object's block
  files, overwrites the key, and reads the already-open handles through
  to completion: same bytes, no new window. The equivalence goes in a
  code comment at the release site, not only in the test.
- **Self-copy no-op**: deferred with component 5.

## Component 7: docs

- `docs/fsck.md`: the "when to run it" bullet that calls the same-key
  overwrite leak the routine reason -- one line, replaced by what is
  now true: successful overwrites release, so steady-state findings on
  a healthy store shrink toward zero and the routine reasons are crash
  residue and cold-data verification.
- `docs/refcount.md`: the counting rule keeps "every dedup hit bumps"
  and loses "an overwrite therefore deliberately over-counts until
  reconciled". Replaced by the ADR 0008 arithmetic: the overwrite
  releases what it displaced, so per shared block the net is unchanged,
  per dropped block -1, per added block +1.
- The ADR's Status line gains the landing series once the last component
  is in (house rule, standalone commit).

## Sequencing and risk

Order: 0 -> 1 -> 2 -> 3 -> 4 -> 6 -> 7, with 5 left open.

Riskiest is component 2: it is the only one that changes what a write
does, and it widens `async` across three crates. Mitigation: the
transaction helper is synchronous and separate, so the ordering rule and
the lock order are both readable in one screen; the race arms of
component 6 are written against the invariants and run before the
tightened assertions of component 4 are trusted.

Component 1 is mechanical but load-bearing: a `replace_object` that
split its read from its write would reintroduce the double-release as a
silent loss bug, which is why it is a transaction method and not a pair
of calls in `CasFS`.
