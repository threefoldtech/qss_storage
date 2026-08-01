//! Per-block striped locking (ADR 0006).
//!
//! Every `_BLOCKS` record mutation -- insert, dedup bump, decrement, remove
//! -- must happen under the stripe of the block's hash (hard rule 1 of the
//! implementation plan), which serializes writers of the SAME block while
//! writers of different blocks proceed in parallel (up to stripe
//! collisions).
//!
//! Sizing (from the ADR): with K concurrent block writers and N stripes,
//! the probability a writer is spuriously serialized behind an unrelated
//! block is about `(K-1)/N`. Scale N to roughly 16x the maximum expected
//! concurrent block writers; the default of 1024 keeps that below ~6% at
//! K=64. N is a store-level option, not per-namespace: all namespaces of
//! one store share the same stripe set, or the locking would not serialize
//! cross-namespace dedup.

use std::sync::Arc;

use crate::metastore::BlockId;

/// Default stripe count. See the module doc for the sizing rule.
///
/// `config::DEFAULT_STRIPE_COUNT` re-exports this rather than repeating the
/// number, so the configured default and the built-in one cannot drift.
pub(crate) const DEFAULT_STRIPE_COUNT: usize = 1024;

/// Largest stripe count [`Stripes::for_hash`] can address.
///
/// The index is the id's first two bytes, so there are exactly 65536 distinct
/// indices however long the vector is: stripes past this are allocated and
/// never taken. There is no power-of-two requirement and no rounding -- any
/// count in `1..=MAX_STRIPE_COUNT` is used in full, because the index is
/// reduced modulo the count. A count of 0 would panic on the modulo, which is
/// why [`Stripes::new`] clamps it; the config layer refuses it outright rather
/// than quietly hand back a single global lock.
pub(crate) const MAX_STRIPE_COUNT: usize = 1 << 16;

/// A fixed set of async mutexes, indexed by block hash.
///
/// [`Stripes::for_hash`] returns a clone of the `Arc` so callers can take
/// the lock with `lock_owned()` -- required because the guard must be able
/// to move INTO a `spawn_blocking` closure (hard rule 4: a stripe guard
/// held by a cancellable future must never span an await whose detached
/// continuation performs a destructive op).
#[derive(Debug)]
pub(crate) struct Stripes {
    locks: Vec<Arc<tokio::sync::Mutex<()>>>,
}

impl Stripes {
    /// Creates `n` stripes; `n` is clamped to at least 1.
    pub fn new(n: usize) -> Self {
        let n = n.max(1);
        Self {
            locks: (0..n)
                .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                .collect(),
        }
    }

    /// The stripe protecting `id`'s block record.
    ///
    /// Index = the id's first two bytes read big-endian, modulo the stripe
    /// count. Block ids are hashes, so the leading bytes are uniform; two
    /// bytes address up to 65536 stripes.
    pub fn for_hash(&self, id: &BlockId) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(&self.locks[self.index_of(id)])
    }

    /// The index [`for_hash`](Self::for_hash) resolves `id` to. Public to the
    /// module because it -- not the hash -- is what the batch acquisition
    /// sorts on; see [`Stripes::lock_batch`].
    fn index_of(&self, id: &BlockId) -> usize {
        let bytes = id.as_slice();
        // BlockId is 16 or 32 bytes wide by construction, never shorter.
        let index = (usize::from(bytes[0]) << 8) | usize::from(bytes[1]);
        index % self.locks.len()
    }

    /// Takes every stripe a batch of block ids needs, as one guard (ADR
    /// 0010).
    ///
    /// # Sorted, and sorted on the LOCK
    ///
    /// The ADR says "sorted block-hash order". This sorts on the stripe
    /// INDEX, which is the same order whenever the batch's hashes address
    /// distinct stripes -- and is the only one that is a total order over the
    /// LOCKS when they do not. Several hashes can share a stripe (the index
    /// is two bytes reduced modulo the count), and then hash order picks
    /// whichever of them happens to be smallest, which differs between two
    /// batches holding different members of the same stripe: batch A takes
    /// S1-then-S2, batch B takes S2-then-S1, and that is the ABBA the sorted
    /// acquisition exists to rule out. Sorting on the index cannot do that,
    /// because the index is a property of the lock and not of the batch.
    ///
    /// Duplicates are collapsed: taking one `tokio::sync::Mutex` twice from
    /// one task is a deadlock against itself, and a batch that contains the
    /// same block twice -- or two blocks colliding on one stripe -- is
    /// routine.
    ///
    /// # Where this sits in the protocol
    ///
    /// Taken AFTER the batch's data syncs (temp files are per-attempt-unique
    /// and invisible to readers, so they need no stripe) and immediately
    /// before the renames and the one transaction; released when the returned
    /// guard drops, which the write path arranges to be after the commit.
    /// Lock order is unchanged from ADR 0006 -- stripes first, fjall second,
    /// just N stripes instead of one.
    pub async fn lock_batch(&self, ids: &[BlockId]) -> BatchStripeGuard {
        let mut wanted: Vec<usize> = ids.iter().map(|id| self.index_of(id)).collect();
        wanted.sort_unstable();
        wanted.dedup();

        let mut guards = Vec::with_capacity(wanted.len());
        for index in wanted {
            guards.push(Arc::clone(&self.locks[index]).lock_owned().await);
        }
        BatchStripeGuard { _guards: guards }
    }
}

/// Every stripe one batch needs, held together and released together.
///
/// Owned guards (not borrows) so the whole set can move INTO the
/// `spawn_blocking` closure that renames the files and commits the
/// transaction -- the same reason the single-block path uses `lock_owned`
/// (ADR 0006 hard rule 4): cancelling the request future past that point
/// abandons a closure that still runs to completion and releases the stripes
/// itself, so no batch is ever half-applied because a client hung up.
#[must_use = "the stripes are released when this guard drops"]
pub(crate) struct BatchStripeGuard {
    _guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::BLOCKID_SIZE;

    fn id(b0: u8, b1: u8) -> BlockId {
        let mut bytes = [0u8; BLOCKID_SIZE];
        bytes[0] = b0;
        bytes[1] = b1;
        BlockId::from(bytes)
    }

    #[test]
    fn same_hash_resolves_to_the_same_lock() {
        let stripes = Stripes::new(DEFAULT_STRIPE_COUNT);
        assert!(Arc::ptr_eq(
            &stripes.for_hash(&id(0xab, 0xcd)),
            &stripes.for_hash(&id(0xab, 0xcd))
        ));
    }

    #[test]
    fn index_is_first_two_bytes_big_endian_mod_n() {
        let stripes = Stripes::new(256);
        // 0x01_02 = 258; 258 % 256 = 2 = 0x00_02.
        assert!(Arc::ptr_eq(
            &stripes.for_hash(&id(0x01, 0x02)),
            &stripes.for_hash(&id(0x00, 0x02))
        ));
        // Differing second byte lands elsewhere.
        assert!(!Arc::ptr_eq(
            &stripes.for_hash(&id(0x00, 0x02)),
            &stripes.for_hash(&id(0x00, 0x03))
        ));
    }

    #[test]
    fn zero_count_is_clamped_not_a_panic() {
        let stripes = Stripes::new(0);
        let _ = stripes.for_hash(&id(0xff, 0xff));
    }

    #[tokio::test]
    async fn the_lock_actually_serializes() {
        let stripes = Stripes::new(4);
        let lock = stripes.for_hash(&id(1, 1));
        let guard = lock.clone().lock_owned().await;
        assert!(
            stripes.for_hash(&id(1, 1)).try_lock().is_err(),
            "same stripe must be held"
        );
        drop(guard);
        assert!(stripes.for_hash(&id(1, 1)).try_lock().is_ok());
    }

    /// A batch guard holds every stripe its ids need, and releasing it
    /// releases all of them.
    #[tokio::test]
    async fn a_batch_guard_holds_every_stripe_it_named() {
        let stripes = Stripes::new(DEFAULT_STRIPE_COUNT);
        let ids = [id(0x11, 0x22), id(0xaa, 0xbb), id(0x00, 0x01)];

        let guard = stripes.lock_batch(&ids).await;
        for one in &ids {
            assert!(
                stripes.for_hash(one).try_lock().is_err(),
                "every stripe in the batch must be held"
            );
        }

        drop(guard);
        for one in &ids {
            assert!(
                stripes.for_hash(one).try_lock().is_ok(),
                "dropping the guard releases all of them"
            );
        }
    }

    /// The self-deadlock case: a batch naming one block twice, and two
    /// blocks colliding onto one stripe. Taking a `tokio` mutex twice from
    /// one task never returns, so the acquisition has to collapse
    /// duplicates rather than count them.
    #[tokio::test]
    async fn duplicate_stripes_are_taken_once() {
        let stripes = Stripes::new(256);
        // 0x0102 = 258 and 0x0002 = 2 both reduce to stripe 2 with 256
        // stripes; the repeated id covers the same-block case.
        let repeated = id(0x07, 0x07);
        let ids = [repeated, id(0x01, 0x02), id(0x00, 0x02), repeated];

        let guard =
            tokio::time::timeout(std::time::Duration::from_secs(5), stripes.lock_batch(&ids))
                .await
                .expect("a batch with duplicate stripes must not deadlock against itself");

        assert!(stripes.for_hash(&id(0x00, 0x02)).try_lock().is_err());
        drop(guard);
    }

    /// Acquisition order is the stripe index, not the hash -- the property
    /// that rules ABBA out. Two batches sharing two stripes but naming
    /// different members of them must still take those stripes in the same
    /// order; sorting on the hash would not guarantee that, because each
    /// batch would sort by its own member.
    ///
    /// Concretely with 256 stripes: `0x00_02` and `0x01_02` are both stripe
    /// 2, `0x00_03` and `0x01_03` are both stripe 3. Batch A names
    /// (0x00_02, 0x01_03), batch B names (0x01_02, 0x00_03) -- in hash order
    /// A takes 0x00_02 first and B takes 0x00_03 first, i.e. A takes stripe
    /// 2 then 3 while B takes 3 then 2. The two run concurrently here: under
    /// hash order this hangs, under index order it completes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_batches_sharing_stripes_do_not_deadlock() {
        let stripes = Arc::new(Stripes::new(256));

        for _ in 0..200 {
            let a = {
                let stripes = Arc::clone(&stripes);
                tokio::spawn(async move {
                    let _g = stripes.lock_batch(&[id(0x00, 0x02), id(0x01, 0x03)]).await;
                    tokio::task::yield_now().await;
                })
            };
            let b = {
                let stripes = Arc::clone(&stripes);
                tokio::spawn(async move {
                    let _g = stripes.lock_batch(&[id(0x01, 0x02), id(0x00, 0x03)]).await;
                    tokio::task::yield_now().await;
                })
            };

            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                a.await.unwrap();
                b.await.unwrap();
            })
            .await
            .expect("opposed batches over the same stripes must not deadlock");
        }
    }

    /// An empty batch takes nothing and blocks on nothing -- the degenerate
    /// case a request whose blocks all deduped inside the accumulator hits.
    #[tokio::test]
    async fn an_empty_batch_takes_no_stripes() {
        let stripes = Stripes::new(16);
        let guard = stripes.lock_batch(&[]).await;
        assert!(stripes.for_hash(&id(0, 0)).try_lock().is_ok());
        drop(guard);
    }
}
