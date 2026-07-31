# The key_has_block skip: mechanics, loss trace, and the decided fix

**Status**: HISTORICAL. The skip this document dissects was removed on
2026-07-31 when ADR 0006's write protocol landed: every dedup hit now
bumps the refcount inside a striped transactional RMW
(`Transaction::bump_block_rc`), and the file:line references below
point at code that no longer exists. Kept as the worked loss example
behind the decision. See `docs/adr/0006-block-write-protocol.md`
(section "key_has_block and same-key overwrite") and
`docs/plans/adr-0006-implementation.md` (Component 5).

---

## What it is

A special case in the PUT path that suppresses the refcount bump when
it believes the bump would double-count.

When `store_object` starts, it reads the object metadata already
present at the destination bucket/key -- the object about to be
overwritten (`cas-storage/src/cas/write_path.rs:78-82`). For each
incoming block it then checks: does that *old* object at this same key
already contain this block hash? That boolean is `key_has_block`
(`write_path.rs:123-127`). It is passed into `write_block`, and when it
is true and the block record exists, the transaction returns "dedup
hit" WITHOUT incrementing the refcount
(`cas-storage/src/metastore/meta_store.rs:636-659` -- the branch logs
"Block exists: NOT incrementing").

## Why it exists

If you PUT the same content to the same key twice, the new object
replaces the old one, and the old object's reference is what the rc
already counts. Nothing ever decrements the replaced object's blocks
(`create_object_meta` blind-upserts, `cas-storage/src/cas/fs.rs:246-258`),
so without the skip every same-content overwrite would permanently
inflate rc by one. The skip says: this key already pays for this block,
do not charge it again.

The accounting rests on two assumptions that are both false under
concurrency and multipart -- see the trace below.

## Today's PUT (pseudocode)

```text
store_object(bucket, key, data):
    old_obj = get_object_meta(bucket, key)        # ONE read, at start,
                                                  # no lock, never refreshed
    for each 1 MiB chunk:
        h = blake3(chunk)
        key_has_block = old_obj != None && old_obj.blocks.contains(h)

        tx = begin_transaction()
        rec = tx.get_block(h)
        if rec exists:
            if key_has_block:
                pass                              # <-- THE SKIP: no rc bump,
                                                  #     "this key already pays"
            else:
                rec.rc += 1
            tx.commit()
            continue                              # dedup hit: no disk write either
        else:
            tx.insert_block(h, rc=1); tx.commit()
            write file                            # (meta-first, today's order)

    create_object_meta(bucket, key, new_blocks)   # blind upsert -- the OLD
                                                  # object's blocks are NEVER
                                                  # decremented
```

## Today's DELETE (pseudocode)

```text
delete_object(bucket, key):                       # meta_store.rs:343-415
    obj = get + remove object record
    for h in obj.blocks:                # per LIST OCCURRENCE -- a block
        rec = get_block(h)              # appearing twice decrements twice
        if rec.rc == 1: remove record; unlink file
        else:           rec.rc -= 1; put_block(rec)
```

## The loss trace

Setup: key K's old object contains block B, and another key K2 also
references B. So `rc(B) = 2`, true references = 2. Now K is re-uploaded
via multipart, and B occurs in two parts:

```text
                                          rc(B)    true refs of B
start                                       2       2  (old-K, K2)
part 1: sees old_obj(K) has B -> SKIP       2       3  (old-K, K2, new-K#1)
part 2: sees old_obj(K) has B -> SKIP       2       4  (old-K, K2, new-K#2)
complete: new K object lists [.., B, .., B, ..]
          old-K record replaced, its refs never decremented
                                            2       3  (K2, new-K#1, new-K#2)
          -- rc already undercounts by 1 --

later: DELETE K
    occurrence 1 of B: rc 2 -> 1            1       1  (K2)
    occurrence 2 of B: rc == 1
        -> remove record, unlink file       -       1  (K2)  <-- LOSS

GET of K2's object -> BlockNotFound
```

The undercount is invisible until the delete fires. Fsck cannot flag it
beforehand: `rc = 2` with objects referencing B looks internally
consistent -- an undercount is indistinguishable from a valid state.
This is the loss class the refcount contract (`docs/refcount.md`)
forbids: leakage (over-count) is allowed, loss (under-count) never.

Two independent failure ingredients, either one suffices:

1. **Stale snapshot.** The old-object read happens once, before any
   locking, and for multipart it happens once per `upload_part`
   request -- each part makes skip decisions against a snapshot a
   concurrent DELETE of that key may have already invalidated.
2. **Occurrence mismatch.** DELETE decrements once per block-list
   occurrence; the skip does not count occurrences. Any object whose
   block list contains a duplicate that the old object also had will
   under-count.

ADR 0006's stripes do NOT fix this by themselves: the stripe serializes
the rc read-modify-write, but the *predicate* deciding whether to bump
is computed from the stale unlocked snapshot far outside the stripe.

## The decided fix (ADR 0006, 2026-07-31): drop the skip

```text
        tx = begin_transaction()          # under stripe(h) in the new design
        rec = tx.get_block(h)
        if rec exists:
            rec.rc += 1                   # ALWAYS -- key_has_block is gone
            tx.commit()
```

Same trace with the fix: part 1 bumps 2 -> 3, part 2 bumps 3 -> 4;
DELETE of K decrements 4 -> 3 -> 2; K2 survives.

The cost runs the other direction: a same-key overwrite with identical
content now leaves rc one higher per overwrite than live references.
That is the same leak class the overwrite path already produces today
for every *changed* block (replaced objects' blocks are never
decremented), so dropping the skip introduces no new defect class -- it
converts a silent, undetectable loss-class under-count into more of a
known leak-class over-count, which ADR 0005's refcount recount
reconciles. The proper long-term pairing -- overwrite decrements the
replaced object's blocks through ADR 0006's striped delete primitive --
is recorded as follow-up work, out of scope for ADR 0006.

That follow-up landed as ADR 0008
(`docs/adr/0008-overwrite-releases-replaced-blocks.md`): an overwrite
releases the replaced record's occurrences through `release_blocks`,
after the new record commits. The cost described above is paid back --
a same-content re-PUT nets to no change -- so nothing of this trade
survives except the fix itself.
