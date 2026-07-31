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
pub(crate) const DEFAULT_STRIPE_COUNT: usize = 1024;

/// A fixed set of async mutexes, indexed by block hash.
///
/// [`Stripes::for_hash`] returns a clone of the `Arc` so callers can take
/// the lock with `lock_owned()` -- required because the guard must be able
/// to move INTO a `spawn_blocking` closure (hard rule 4: a stripe guard
/// held by a cancellable future must never span an await whose detached
/// continuation performs a destructive op).
// TODO(adr-0006): the allow dies when the write path (component 5) starts
// taking stripes.
#[allow(dead_code)]
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
    // TODO(adr-0006): the allow dies when the write path (component 5)
    // starts taking stripes.
    #[allow(dead_code)]
    pub fn for_hash(&self, id: &BlockId) -> Arc<tokio::sync::Mutex<()>> {
        let bytes = id.as_slice();
        // BlockId is 16 or 32 bytes wide by construction, never shorter.
        let index = (usize::from(bytes[0]) << 8) | usize::from(bytes[1]);
        Arc::clone(&self.locks[index % self.locks.len()])
    }
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
}
