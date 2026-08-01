//! The fjall metadata backend: fjall's single-writer transactional database.
//!
//! This was once two backends (transactional and not) sharing a generic
//! flavor layer in `fjall_common.rs`; ADR 0007 removed the non-transactional
//! one and the generic layer was folded back in here. Writes go through a
//! real fjall write transaction, so a rollback discards them without touching
//! the keyspace; the price is that only one writer may be in flight at a
//! time.

use std::collections::HashMap;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use fjall::{self, KeyspaceCreateOptions, Readable, SingleWriterTxKeyspace};

use crate::metastore::{
    BaseMetaTree, Durability, KeyValuePairs, MetaError, MetaTreeExt, Object, Store, Transaction,
    TransactionBackend,
};

/// Objects at or below this size have their data inlined in the metadata
/// record instead of being written to the block store. Set very low so that
/// inlining is practically disabled unless a caller asks for it.
pub const DEFAULT_INLINED_METADATA_SIZE: usize = 1;

/// Database handle: the fjall database plus the persist mode applied after
/// every commit.
#[derive(Clone)]
pub struct TxDb {
    db: Arc<fjall::SingleWriterTxDatabase>,
    durability: fjall::PersistMode,
}

impl TxDb {
    fn commit_persist(&self, tx: fjall::SingleWriterWriteTx) -> Result<(), MetaError> {
        tx.commit()
            .map_err(|e| MetaError::TransactionError(e.to_string()))?;

        self.db
            .persist(self.durability)
            .map_err(|e| MetaError::PersistError(e.to_string()))?;
        Ok(())
    }
}

/// Metadata store backed by fjall's single-writer transactional database.
#[derive(Clone)]
pub struct FjallStore {
    db: TxDb,
    inlined_metadata_size: usize,
    /// Keyspace handles are cached by name so that a hot path does not take
    /// the database's keyspace lock on every tree access. Entries are evicted
    /// by [`Store::tree_delete`].
    partition_cache: Arc<Mutex<HashMap<String, Arc<SingleWriterTxKeyspace>>>>,
}

impl std::fmt::Debug for FjallStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallStore")
            .field("db", &"<fjall::SingleWriterTxDatabase>")
            .finish()
    }
}

impl FjallStore {
    /// Opens (creating if absent) the fjall database at `path`.
    ///
    /// # Errors
    ///
    /// [`MetaError::StoreLocked`] if another process already holds the
    /// database's lock -- fjall takes an exclusive lock on the directory, so
    /// this is the routine answer whenever the daemon is running and an
    /// offline tool is pointed at its store. Any other open failure becomes
    /// [`MetaError::OtherDBError`].
    ///
    /// This used to `.unwrap()`. Lock contention is an expected condition, not
    /// a bug, and the tools' exit-code contracts (fsck's exit 3) depend on it
    /// arriving as a value.
    pub fn new(
        path: PathBuf,
        inlined_metadata_size: Option<usize>,
        durability: Option<Durability>,
    ) -> Result<Self, MetaError> {
        tracing::debug!("Opening fjall store at {:?}", path);

        let db = fjall::SingleWriterTxDatabase::builder(&path)
            .open()
            .map_err(|e| match e {
                fjall::Error::Locked => MetaError::StoreLocked(path.display().to_string()),
                other => MetaError::OtherDBError(format!(
                    "cannot open metadata store at {}: {other}",
                    path.display()
                )),
            })?;

        // Two levels since ADR 0010. `fsync` persists the journal with data
        // AND metadata (fjall SyncAll, the strongest mode); `buffer` leaves
        // the flush to the page cache. fjall's SyncData has no user-facing
        // level any more: on an append-only journal it must flush the size
        // metadata anyway, so it bought nothing that SyncAll does not.
        let durability = match durability.unwrap_or(Durability::Fsync) {
            Durability::Buffer => fjall::PersistMode::Buffer,
            Durability::Fsync => fjall::PersistMode::SyncAll,
        };

        Ok(Self {
            db: TxDb {
                db: Arc::new(db),
                durability,
            },
            inlined_metadata_size: inlined_metadata_size.unwrap_or(DEFAULT_INLINED_METADATA_SIZE),
            partition_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn db(&self) -> &TxDb {
        &self.db
    }

    pub fn get_inlined_metadata_size(&self) -> usize {
        self.inlined_metadata_size
    }

    /// Returns the cached handle for `name`, opening (and creating) the
    /// keyspace on first use.
    pub fn get_partition(&self, name: &str) -> Result<Arc<SingleWriterTxKeyspace>, MetaError> {
        let mut cache = self
            .partition_cache
            .lock()
            .expect("Can lock partition cache");
        if let Some(partition) = cache.get(name) {
            return Ok(partition.clone());
        }
        let partition = Arc::new(
            self.db
                .db
                .keyspace(name, KeyspaceCreateOptions::default)
                .map_err(|e| MetaError::OtherDBError(e.to_string()))?,
        );
        cache.insert(name.to_string(), partition.clone());
        Ok(partition)
    }

    /// Exact key count of one keyspace, via a read transaction.
    fn count_keys(&self, partition: &SingleWriterTxKeyspace) -> Result<usize, MetaError> {
        self.db
            .db
            .read_tx()
            .len(partition)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))
    }
}

impl Store for FjallStore {
    fn tree_open(&self, name: &str) -> Result<Arc<dyn BaseMetaTree>, MetaError> {
        Ok(Arc::new(FjallTree::new(
            self.db.clone(),
            self.get_partition(name)?,
        )))
    }

    fn tree_ext_open(&self, name: &str) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError> {
        Ok(Arc::new(FjallTree::new(
            self.db.clone(),
            self.get_partition(name)?,
        )))
    }

    fn tree_exists(&self, name: &str) -> Result<bool, MetaError> {
        Ok(self.db.db.keyspace_exists(name))
    }

    // The database's own keyspace registry is the authority, not the
    // partition cache: a tree this process never opened is still a tree.
    fn list_trees(&self) -> Result<Vec<String>, MetaError> {
        Ok(self
            .db
            .db
            .list_keyspace_names()
            .iter()
            .map(ToString::to_string)
            .collect())
    }

    fn tree_delete(&self, name: &str) -> Result<(), MetaError> {
        let partition = self.get_partition(name)?;
        // Drop the cached handle first: after the delete it refers to a
        // keyspace that no longer exists.
        self.partition_cache
            .lock()
            .expect("Can lock partition cache")
            .remove(name);
        // `delete_keyspace` lives on the plain database underneath and takes a
        // plain keyspace handle by value.
        self.db
            .db
            .inner()
            .delete_keyspace(partition.inner().clone())
            .map_err(|e| MetaError::OtherDBError(e.to_string()))
    }

    fn begin_transaction(&self) -> Transaction {
        tracing::debug!(target: "cas_storage::locks", "Transaction started");
        // Upstream's comment here was "the transaction won't outlive the store",
        // which states the conclusion without the two facts it rests on. Both are
        // spelled out below, because both are silently breakable by an unrelated
        // edit.
        //
        // SAFETY: `write_tx()` borrows the `SingleWriterTxDatabase`, and that
        // borrow is laundered to `'static` here. Two properties make the
        // laundered lifetime true in practice:
        //
        // 1. Liveness. The `FjallTransaction` built on the next line owns an
        //    `Arc<FjallStore>` cloned from `self`, and the store's database
        //    handle (`TxDb`) holds an `Arc<SingleWriterTxDatabase>`. The
        //    database therefore stays alive for at least as long as the
        //    transaction, whatever happens to the `FjallStore` this was
        //    called on.
        // 2. Drop order. `FjallTransaction` declares `tx` before `store`, and
        //    Rust drops struct fields in declaration order, so the transaction
        //    (and the single-writer lock guard inside it) is released before the
        //    `Arc<FjallStore>` that keeps the database alive. See the field-order
        //    note on the struct.
        //
        // Neither property is enforced by the compiler. Removing the `Arc` from
        // `FjallTransaction`, or swapping its two fields, reintroduces a
        // use-after-free without any diagnostic.
        let tx = unsafe {
            std::mem::transmute::<fjall::SingleWriterWriteTx<'_>, fjall::SingleWriterWriteTx<'static>>(
                self.db.db.write_tx(),
            )
        };

        Transaction::new(Box::new(FjallTransaction::new(tx, Arc::new(self.clone()))))
    }

    // Upstream leaves `Store::num_keys` `unimplemented!()` on this backend,
    // which panics on the default `--metadata-db fjall` path. A read
    // transaction counts a keyspace fine -- it is the same call `len` uses.
    fn num_keys(&self, tree_name: &str) -> Result<usize, MetaError> {
        let partition = self.get_partition(tree_name)?;
        self.count_keys(&partition)
    }

    fn disk_space(&self) -> u64 {
        self.db.db.disk_space().unwrap_or(0)
    }
}

/// A metadata tree: one keyspace of the fjall database, plus the database
/// handle needed to read from it.
///
/// The transactional keyspace handle has no iteration API of its own, so
/// reads go through a read transaction on the database. The returned
/// [`fjall::Iter`] owns its snapshot nonce and so stays valid after the read
/// transaction that produced it is dropped.
pub struct FjallTree {
    db: TxDb,
    partition: Arc<SingleWriterTxKeyspace>,
}

impl FjallTree {
    pub fn new(db: TxDb, partition: Arc<SingleWriterTxKeyspace>) -> Self {
        Self { db, partition }
    }

    /// Forward-ordered iterator over a key range.
    fn range<R: RangeBounds<Vec<u8>>>(
        db: &TxDb,
        partition: &SingleWriterTxKeyspace,
        range: R,
    ) -> fjall::Iter {
        db.db.read_tx().range::<Vec<u8>, _>(partition, range)
    }

    /// Iterator over all keys carrying `prefix`.
    fn prefix(db: &TxDb, partition: &SingleWriterTxKeyspace, prefix: &[u8]) -> fjall::Iter {
        db.db.read_tx().prefix(partition, prefix)
    }
}

impl BaseMetaTree for FjallTree {
    fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<(), MetaError> {
        self.partition
            .insert(key, value)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))
    }

    fn remove(&self, key: &[u8]) -> Result<bool, MetaError> {
        // fjall's remove does not say whether the key was there, so the
        // existence is probed first; the two ops are not one transaction,
        // which is the "best-effort" in the trait contract.
        let existed = self
            .partition
            .contains_key(key)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))?;
        self.partition
            .remove(key)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))?;
        Ok(existed)
    }

    fn contains_key(&self, key: &[u8]) -> Result<bool, MetaError> {
        self.partition
            .contains_key(key)
            .map_err(|_| MetaError::KeyNotFound)
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        match self.partition.get(key) {
            Ok(v) => Ok(v.map(|v| v.to_vec())),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    // Upstream marks `len` `#[cfg(test)]`; it is needed at runtime here (see
    // EXTENSIONS.md).
    fn len(&self) -> Result<usize, MetaError> {
        self.db
            .db
            .read_tx()
            .len(&*self.partition)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))
    }
}

impl MetaTreeExt for FjallTree {
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

            Self::range(&db, &partition, range)
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

            Self::range(&db, &partition, range)
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
                    Box::new(Self::prefix(db, partition, prefix.as_bytes()))
                }
                (Some(prefix), _) => Box::new(Self::prefix(db, partition, prefix.as_bytes())),
                (None, Some(ctsa)) => {
                    let mut next_key = ctsa.as_bytes().to_vec();
                    next_key.push(0);
                    Box::new(Self::range(db, partition, next_key..))
                }
                (None, None) => Box::new(Self::range(db, partition, ..)),
            };

        let pairs = base_iter.filter_map(|g| g.into_inner().ok());

        let skip_filtered = if let (Some(_), Some(ctsa)) = (&prefix, ctsa) {
            let ctsa_bytes = ctsa.into_bytes();
            Box::new(pairs.skip_while(move |(raw_key, _)| &**raw_key <= ctsa_bytes.as_slice()))
                as Box<dyn Iterator<Item = _>>
        } else {
            Box::new(pairs)
        };

        // Upstream used `String::from_utf8_unchecked` on the raw key and
        // `unwrap()`ed the value decode. Both come straight off disk, so a
        // corrupt or truncated record turned into undefined behaviour or a
        // panic instead of a bad result. `range_filter` yields an infallible
        // item type, so a key that is not valid UTF-8 and a value that fails to
        // decode are both skipped and logged -- the same treatment the iterator
        // above already gives to keys the backend fails to read
        // (`filter_map(|g| g.into_inner().ok())`).
        Box::new(skip_filtered.filter_map(|(raw_key, raw_value)| {
            let key = match String::from_utf8(raw_key.to_vec()) {
                Ok(key) => key,
                Err(e) => {
                    tracing::error!("Skipping key that is not valid UTF-8: {}", e);
                    return None;
                }
            };
            let obj = match Object::try_from(&*raw_value) {
                Ok(obj) => obj,
                Err(e) => {
                    tracing::error!("Skipping key {} with an undecodable object: {}", key, e);
                    return None;
                }
            };
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

pub struct FjallTransaction {
    // FIELD ORDER IS LOAD-BEARING. DO NOT REORDER.
    //
    // `tx` holds a transaction whose lifetime was laundered to `'static` in
    // `Store::begin_transaction`; it borrows the database owned (via
    // `Arc`) by `store`. Fields drop in declaration order, so `tx` must be
    // declared first to guarantee the transaction is released before the
    // `Arc<FjallStore>` that keeps the database alive. Swapping these two lines
    // produces a use-after-free that the compiler cannot see, because the
    // `'static` in the type is a lie the `unsafe` block told it.
    tx: Option<fjall::SingleWriterWriteTx<'static>>,
    store: Arc<FjallStore>,
}

impl FjallTransaction {
    pub fn new(tx: fjall::SingleWriterWriteTx<'static>, store: Arc<FjallStore>) -> Self {
        Self {
            tx: Some(tx),
            store,
        }
    }
}

// SAFETY: `FjallTransaction` is `Send` by assertion, not by derivation. The
// blocker is `fjall::SingleWriterWriteTx`, which holds a
// `std::sync::MutexGuard<'_, ()>` for fjall's single-writer lock, and std's
// guard is `!Send`. Everything else in the struct (`Arc<FjallStore>`, the
// transaction's `Database` handle, its memtables and snapshot nonce) is already
// `Send + Sync`.
//
// This impl is REQUIRED: `TransactionBackend` has a `Send + Sync` supertrait
// bound, so `Box<dyn TransactionBackend>` will not accept `FjallTransaction`
// without it. Deleting it fails the build.
//
// What makes it sound, stated as plainly as it can be: the single-writer lock
// means at most one `FjallTransaction` exists at a time, so there is no shared
// mutable state to race on, and the transaction's own data is thread-agnostic.
// The residual assumption is about the guard, not the data -- a mutex guard may
// only be released on the thread that acquired it on platforms whose mutex is
// thread-affine (POSIX `pthread_mutex_t`). Rust's std makes no promise either
// way, which is exactly why the guard is `!Send`.
//
// In this codebase the assumption holds because of how transactions are used,
// not because of anything the type enforces: the only caller
// (`cas::write_path::store_object`) begins a transaction and commits or drops it
// within one uninterrupted synchronous stretch, with no `.await` in between, so
// the guard is always released on the thread that took it. On Linux, std's mutex
// is a futex and cross-thread release is fine regardless.
//
// If a future ever holds a `Transaction` across an `.await`, a tokio worker
// steal can move the guard to another thread, and this impl stops being a
// formality and starts being a real, platform-dependent claim. Do not do that
// without revisiting this comment.
unsafe impl Send for FjallTransaction {}

// `unsafe impl Sync for FjallTransaction {}` was here and has been DELETED.
// It was never needed: `MutexGuard<'_, T>` is `Sync` when `T: Sync`, so
// `FjallTransaction` gets a perfectly ordinary auto `Sync` impl, and the
// `Send + Sync` supertrait bound is satisfied without any assertion. Every
// `TransactionBackend` method takes `&mut self` in any case, so shared-reference
// access does not arise. Removing it restores the compiler's ability to notice
// if a future field makes shared access unsound.
//
// The assertion below pins that claim: if a field is ever added that is not
// `Sync`, this fails at the definition instead of being papered over.
const _: () = {
    const fn assert_sync<T: Sync>() {}
    assert_sync::<FjallTransaction>();
};

impl TransactionBackend for FjallTransaction {
    fn commit(&mut self) -> Result<(), MetaError> {
        if let Some(tx) = self.tx.take() {
            tracing::debug!(target: "cas_storage::locks", "Transaction commit started");
            let res = self.store.db().commit_persist(tx);
            tracing::debug!(target: "cas_storage::locks", "Transaction commit finished");
            res
        } else {
            Err(MetaError::TransactionError(
                "Transaction already rolled back".to_string(),
            ))
        }
    }

    fn rollback(&mut self) {
        if let Some(tx) = self.tx.take() {
            tracing::debug!(target: "cas_storage::locks", "Transaction rollback");
            tx.rollback();
        }
    }

    fn get(&mut self, tree_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        let partition = self.store.get_partition(tree_name)?;
        if let Some(ref mut tx) = self.tx {
            match tx.get(&*partition, key) {
                Ok(Some(data)) => Ok(Some(data.to_vec())),
                Ok(None) => Ok(None),
                Err(e) => Err(MetaError::OtherDBError(e.to_string())),
            }
        } else {
            Err(MetaError::TransactionError(
                "Transaction already rolled back".to_string(),
            ))
        }
    }

    fn insert(&mut self, tree_name: &str, block_id: &[u8], data: Vec<u8>) -> Result<(), MetaError> {
        let partition = self.store.get_partition(tree_name)?;
        if let Some(ref mut tx) = self.tx {
            tx.insert(&partition, block_id, data);
            Ok(())
        } else {
            Err(MetaError::TransactionError(
                "Transaction already rolled back".to_string(),
            ))
        }
    }

    fn remove(&mut self, tree_name: &str, key: &[u8]) -> Result<(), MetaError> {
        let partition = self.store.get_partition(tree_name)?;
        if let Some(ref mut tx) = self.tx {
            tx.remove(&partition, key);
            Ok(())
        } else {
            Err(MetaError::TransactionError(
                "Transaction already rolled back".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    crate::metastore::stores::test_utils::backend_test_battery!(FjallStore, || {
        let dir = tempdir().unwrap();
        let store = FjallStore::new(dir.path().to_path_buf(), Some(1), None).unwrap();
        (store, dir)
    });

    /// Lock contention is a value, not a panic.
    ///
    /// fjall locks the database directory with `std::fs::File::try_lock`,
    /// which is `flock` on Linux -- the lock belongs to the open file
    /// description, so a second open contends even from inside one process.
    /// That is what makes this testable here rather than only across
    /// processes.
    ///
    /// This open used to `.unwrap()`. The tools' exit-code contracts depend on
    /// the error arriving as a value: fsck answers a locked store with exit 3
    /// (`docs/fsck.md`, ADR 0005), and it cannot do that from a panic.
    #[test]
    fn a_second_open_reports_the_lock_instead_of_panicking() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        let _held = FjallStore::new(path.clone(), Some(1), None).expect("the first open must win");

        let err = FjallStore::new(path.clone(), Some(1), None)
            .expect_err("a second open of a held database must fail");

        assert!(
            matches!(err, MetaError::StoreLocked(ref p) if p == &path.display().to_string()),
            "expected StoreLocked naming {}, got {err:?}",
            path.display()
        );

        // The message is what an operator reads off a terminal, so it has to
        // say what to do about it rather than just name a condition.
        let msg = err.to_string();
        assert!(msg.contains("locked by another process"), "{msg}");
        assert!(msg.contains("daemon"), "{msg}");
    }

    /// Dropping the holder releases the lock, so the refusal above is about
    /// contention and not about the store being permanently unusable.
    #[test]
    fn the_lock_is_released_when_the_store_is_dropped() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        let held = FjallStore::new(path.clone(), Some(1), None).unwrap();
        assert!(FjallStore::new(path.clone(), Some(1), None).is_err());

        drop(held);
        FjallStore::new(path, Some(1), None).expect("the lock must be released on drop");
    }
}
