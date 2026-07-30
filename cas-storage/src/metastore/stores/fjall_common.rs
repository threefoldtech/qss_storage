//! Code shared by the two fjall store backends.
//!
//! [`super::fjall`] (transactional: fjall's single-writer transactional
//! database) and [`super::fjall_notx`] (plain database, rollback emulated by
//! the store) used to be near-copies of each other -- same function inventory,
//! ~235 identical lines. They differ in exactly two things:
//!
//! 1. the fjall database flavor they open, and with it the types of the
//!    database and keyspace handles and the way a read is issued;
//! 2. the transaction backend handed to [`Store::begin_transaction`].
//!
//! Everything else -- partition caching, tree opening, `BaseMetaTree`,
//! `MetaTreeExt`, disk-space reporting, the inlined-metadata threshold -- is
//! identical and lives here, generic over the [`FjallFlavor`] trait. Each
//! backend module then contains only its flavor impl, its transaction backend,
//! and its constructor.
//!
//! Nothing in this module is reachable from outside the crate: `stores` is a
//! private module and only the two concrete store aliases are re-exported.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

use crate::metastore::{
    BaseMetaTree, KeyValuePairs, MetaError, MetaTreeExt, Object, Store, Transaction,
};

/// Objects at or below this size have their data inlined in the metadata
/// record instead of being written to the block store. Set very low so that
/// inlining is practically disabled unless a caller asks for it.
pub const DEFAULT_INLINED_METADATA_SIZE: usize = 1;

/// The parts of a fjall backend that the two flavors do not share.
///
/// Implementors are zero-sized marker types; every method is effectively a
/// free function over the flavor's own handle types. [`Db`](Self::Db) is the
/// database handle (cheap to clone, shared by the store and every tree it
/// hands out) and [`Partition`](Self::Partition) is a handle to one named
/// keyspace inside it.
///
/// The read methods differ between flavors because the transactional
/// keyspace handle has no iteration API of its own -- reads there go through
/// a read transaction on the database, while the plain keyspace iterates
/// directly. Both produce a plain [`fjall::Iter`], which owns its snapshot
/// nonce and so outlives the read transaction that produced it.
pub trait FjallFlavor: Sized + 'static {
    /// Struct name used by the store's `Debug` impl.
    const STORE_NAME: &'static str;
    /// Placeholder printed for the (non-`Debug`) database handle.
    const DB_LABEL: &'static str;

    /// Database handle. Cloning it must be cheap and must share state.
    type Db: Clone + Send + Sync + 'static;
    /// Handle to one named keyspace ("partition") of the database.
    type Partition: Send + Sync + 'static;

    // ---- database-level ----

    /// Opens the named keyspace, creating it if it does not exist.
    fn keyspace(db: &Self::Db, name: &str) -> Result<Self::Partition, MetaError>;

    /// Whether a keyspace with this name exists.
    fn keyspace_exists(db: &Self::Db, name: &str) -> bool;

    /// Drops the keyspace from the database.
    fn delete_keyspace(db: &Self::Db, partition: &Self::Partition) -> Result<(), MetaError>;

    /// Disk space used by the whole database, in bytes.
    fn disk_space(db: &Self::Db) -> u64;

    /// Number of keys in `partition`, as reported by [`Store::num_keys`].
    ///
    /// The two flavors differ here on purpose: the transactional one counts
    /// exactly through a read transaction, the plain one reports fjall's
    /// approximation. See `EXTENSIONS.md`.
    fn num_keys(db: &Self::Db, partition: &Self::Partition) -> Result<usize, MetaError>;

    /// Starts a transaction against `store`. This is the other half of what
    /// makes the two backends distinct: a real fjall write transaction versus
    /// a no-op transaction that undoes its own inserts on rollback.
    fn begin_transaction(store: &FjallStoreOf<Self>) -> Transaction;

    // ---- keyspace-level ----

    fn get(partition: &Self::Partition, key: &[u8]) -> Result<Option<fjall::Slice>, MetaError>;

    fn insert(partition: &Self::Partition, key: &[u8], value: Vec<u8>) -> Result<(), MetaError>;

    fn remove(partition: &Self::Partition, key: &[u8]) -> Result<(), MetaError>;

    fn contains_key(partition: &Self::Partition, key: &[u8]) -> Result<bool, MetaError>;

    /// Exact key count, as reported by [`BaseMetaTree::len`]. Unlike
    /// [`num_keys`](Self::num_keys) this is exact on both flavors; respd's
    /// `DBSIZE` and `LENGTH` go through here.
    fn len(db: &Self::Db, partition: &Self::Partition) -> Result<usize, MetaError>;

    /// Forward-ordered iterator over a key range.
    fn range<R: RangeBounds<Vec<u8>>>(
        db: &Self::Db,
        partition: &Self::Partition,
        range: R,
    ) -> fjall::Iter;

    /// Iterator over all keys carrying `prefix`.
    fn prefix(db: &Self::Db, partition: &Self::Partition, prefix: &[u8]) -> fjall::Iter;
}

/// A fjall-backed [`Store`], generic over the backend flavor.
///
/// The two public store types are aliases of this: `FjallStore` (see
/// [`super::fjall`]) and `FjallStoreNotx` (see [`super::fjall_notx`]).
pub struct FjallStoreOf<F: FjallFlavor> {
    db: F::Db,
    inlined_metadata_size: usize,
    /// Keyspace handles are cached by name so that a hot path does not take
    /// the database's keyspace lock on every tree access. Entries are evicted
    /// by [`Store::tree_delete`].
    partition_cache: Arc<Mutex<HashMap<String, Arc<F::Partition>>>>,
}

impl<F: FjallFlavor> Clone for FjallStoreOf<F> {
    // Derived `Clone` would demand `F: Clone`, which is meaningless for a
    // marker type that is never instantiated.
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            inlined_metadata_size: self.inlined_metadata_size,
            partition_cache: self.partition_cache.clone(),
        }
    }
}

impl<F: FjallFlavor> std::fmt::Debug for FjallStoreOf<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(F::STORE_NAME)
            .field("db", &F::DB_LABEL)
            .finish()
    }
}

impl<F: FjallFlavor> FjallStoreOf<F> {
    /// Wraps an already-opened database handle. Each flavor's own `new`
    /// (which is what callers use) opens the database and delegates here.
    pub fn from_db(db: F::Db, inlined_metadata_size: Option<usize>) -> Self {
        Self {
            db,
            inlined_metadata_size: inlined_metadata_size.unwrap_or(DEFAULT_INLINED_METADATA_SIZE),
            partition_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn db(&self) -> &F::Db {
        &self.db
    }

    pub fn get_inlined_metadata_size(&self) -> usize {
        self.inlined_metadata_size
    }

    /// Returns the cached handle for `name`, opening (and creating) the
    /// keyspace on first use.
    pub fn get_partition(&self, name: &str) -> Result<Arc<F::Partition>, MetaError> {
        let mut cache = self
            .partition_cache
            .lock()
            .expect("Can lock partition cache");
        if let Some(partition) = cache.get(name) {
            return Ok(partition.clone());
        }
        let partition = Arc::new(F::keyspace(&self.db, name)?);
        cache.insert(name.to_string(), partition.clone());
        Ok(partition)
    }
}

impl<F: FjallFlavor> Store for FjallStoreOf<F> {
    fn tree_open(&self, name: &str) -> Result<Arc<dyn BaseMetaTree>, MetaError> {
        Ok(Arc::new(FjallTreeOf::<F>::new(
            self.db.clone(),
            self.get_partition(name)?,
        )))
    }

    fn tree_ext_open(&self, name: &str) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError> {
        Ok(Arc::new(FjallTreeOf::<F>::new(
            self.db.clone(),
            self.get_partition(name)?,
        )))
    }

    fn tree_exists(&self, name: &str) -> Result<bool, MetaError> {
        Ok(F::keyspace_exists(&self.db, name))
    }

    fn tree_delete(&self, name: &str) -> Result<(), MetaError> {
        let partition = self.get_partition(name)?;
        // Drop the cached handle first: after the delete it refers to a
        // keyspace that no longer exists.
        self.partition_cache
            .lock()
            .expect("Can lock partition cache")
            .remove(name);
        F::delete_keyspace(&self.db, &partition)
    }

    fn begin_transaction(&self) -> Transaction {
        F::begin_transaction(self)
    }

    fn num_keys(&self, tree_name: &str) -> Result<usize, MetaError> {
        let partition = self.get_partition(tree_name)?;
        F::num_keys(&self.db, &partition)
    }

    fn disk_space(&self) -> u64 {
        F::disk_space(&self.db)
    }
}

/// A metadata tree: one keyspace of a fjall database, plus the database handle
/// the flavor needs to read from it.
pub struct FjallTreeOf<F: FjallFlavor> {
    db: F::Db,
    partition: Arc<F::Partition>,
}

impl<F: FjallFlavor> FjallTreeOf<F> {
    pub fn new(db: F::Db, partition: Arc<F::Partition>) -> Self {
        Self { db, partition }
    }
}

impl<F: FjallFlavor> BaseMetaTree for FjallTreeOf<F> {
    fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<(), MetaError> {
        F::insert(&self.partition, key, value)
    }

    fn remove(&self, key: &[u8]) -> Result<(), MetaError> {
        F::remove(&self.partition, key)
    }

    fn contains_key(&self, key: &[u8]) -> Result<bool, MetaError> {
        F::contains_key(&self.partition, key)
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        Ok(F::get(&self.partition, key)?.map(|v| v.to_vec()))
    }

    // Upstream marks `len` `#[cfg(test)]`; it is needed at runtime here (see
    // EXTENSIONS.md).
    fn len(&self) -> Result<usize, MetaError> {
        F::len(&self.db, &self.partition)
    }
}

impl<F: FjallFlavor> MetaTreeExt for FjallTreeOf<F> {
    fn iter_all(&self) -> KeyValuePairs {
        self.iter_kv(None)
    }

    fn iter_kv(&self, start_after: Option<Vec<u8>>) -> KeyValuePairs {
        let partition = self.partition.clone();
        let db = self.db.clone();
        let mut last_key = start_after;

        Box::new(std::iter::from_fn(move || {
            let range = match &last_key {
                Some(k) => {
                    let mut next = k.clone();
                    next.push(0);
                    next..
                }
                None => Vec::new()..,
            };

            F::range(&db, &partition, range)
                .next()
                .map(|guard| advance(guard, &mut last_key))
        }))
    }

    fn iter_kv_backward(&self, start_key: Option<Vec<u8>>) -> KeyValuePairs {
        let partition = self.partition.clone();
        let db = self.db.clone();
        let mut last_key = start_key;

        Box::new(std::iter::from_fn(move || {
            let range = match &last_key {
                Some(k) => ..k.clone(),
                None => ..Vec::new(),
            };

            F::range(&db, &partition, range)
                .next_back()
                .map(|guard| advance(guard, &mut last_key))
        }))
    }

    // rules:
    // 1. continuation_token and start_after exists: use the one with the highest lexicographical order
    //    -> call it: ctsa
    // 2. if prefix exists
    //    -> ctsa > the prefix && doesn't have prefix: return zero results
    //    -> ctsa < prefix: ignore it
    //    -> ctsa has the prefix: use it as start_after
    //          In kv store like fjall & Sled: we process it in the Rust code
    fn range_filter<'a>(
        &'a self,
        start_after: Option<String>,
        prefix: Option<String>,
        continuation_token: Option<String>,
    ) -> Box<dyn Iterator<Item = (String, Object)> + 'a> {
        let mut ctsa = match (continuation_token, start_after) {
            (Some(token), Some(start)) => Some(std::cmp::max(token, start)),
            (Some(token), None) => Some(token),
            (None, start) => start,
        };

        let db = &self.db;
        let partition = &self.partition;

        let base_iter: Box<dyn Iterator<Item = fjall::Guard>> =
            match (prefix.as_ref(), ctsa.as_ref()) {
                (Some(prefix), Some(ctsa)) if (ctsa > prefix && !ctsa.starts_with(prefix)) => {
                    //Return empty iterator if ctsa is after prefix
                    Box::new(std::iter::empty())
                }
                (Some(prefix), Some(ctsa_local)) if ctsa_local < prefix => {
                    // If ctsa is before prefix, ignore ctsa
                    ctsa = None;
                    Box::new(F::prefix(db, partition, prefix.as_bytes()))
                }
                (Some(prefix), _) => Box::new(F::prefix(db, partition, prefix.as_bytes())),
                (None, Some(ctsa)) => {
                    let mut next_key = ctsa.as_bytes().to_vec();
                    next_key.push(0);
                    Box::new(F::range(db, partition, next_key..))
                }
                (None, None) => Box::new(F::range(db, partition, ..)),
            };

        let pairs = base_iter.filter_map(|g| g.into_inner().ok());

        let skip_filtered = if let (Some(_), Some(ctsa)) = (&prefix, ctsa) {
            let ctsa_bytes = ctsa.into_bytes();
            Box::new(pairs.skip_while(move |(raw_key, _)| &**raw_key <= ctsa_bytes.as_slice()))
                as Box<dyn Iterator<Item = _>>
        } else {
            Box::new(pairs)
        };

        // Upstream used `String::from_utf8_unchecked` on the raw key. The key
        // comes straight off disk, so a corrupt or truncated record turns into
        // undefined behaviour instead of a bad result. `range_filter` yields an
        // infallible item type, so a key that is not valid UTF-8 is skipped and
        // logged -- the same treatment the iterator above already gives to keys
        // the backend fails to read (`filter_map(|g| g.into_inner().ok())`).
        Box::new(skip_filtered.filter_map(|(raw_key, raw_value)| {
            let key = match String::from_utf8(raw_key.to_vec()) {
                Ok(key) => key,
                Err(e) => {
                    tracing::error!("Skipping key that is not valid UTF-8: {}", e);
                    return None;
                }
            };
            let obj = Object::try_from(&*raw_value).unwrap();
            Some((key, obj))
        }))
    }
}

/// Decodes one guard into an owned key-value pair and records the key as the
/// cursor for the next step of an `iter_kv`/`iter_kv_backward` walk.
fn advance(
    guard: fjall::Guard,
    cursor: &mut Option<Vec<u8>>,
) -> Result<(Vec<u8>, Vec<u8>), MetaError> {
    match guard.into_inner() {
        Ok((k, v)) => {
            *cursor = Some(k.to_vec());
            Ok((k.to_vec(), v.to_vec()))
        }
        Err(e) => {
            tracing::error!("Error reading key: {}", e);
            Err(MetaError::OtherDBError(e.to_string()))
        }
    }
}
