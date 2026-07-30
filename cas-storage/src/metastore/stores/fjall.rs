//! The transactional fjall backend.
//!
//! Everything that is not specific to fjall's single-writer transactional
//! database lives in [`super::fjall_common`]; this module holds the flavor
//! impl, the write-transaction backend, and the constructor.

use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;

use fjall::{self, KeyspaceCreateOptions, Readable, SingleWriterTxKeyspace};

use crate::metastore::{Durability, MetaError, Transaction, TransactionBackend};

use super::fjall_common::{FjallFlavor, FjallStoreOf};

/// Metadata store backed by fjall's single-writer transactional database.
///
/// Writes go through a real fjall write transaction, so a rollback discards
/// them without touching the keyspace; the price is that only one writer may
/// be in flight at a time.
pub type FjallStore = FjallStoreOf<Transactional>;

/// Database handle for the transactional flavor: the fjall database plus the
/// persist mode applied after every commit.
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

/// Flavor marker for the transactional backend. Never instantiated.
#[derive(Debug, Clone, Copy)]
pub struct Transactional;

impl FjallFlavor for Transactional {
    const STORE_NAME: &'static str = "FjallStore";
    const DB_LABEL: &'static str = "<fjall::SingleWriterTxDatabase>";

    type Db = TxDb;
    type Partition = SingleWriterTxKeyspace;

    fn keyspace(db: &Self::Db, name: &str) -> Result<Self::Partition, MetaError> {
        db.db
            .keyspace(name, KeyspaceCreateOptions::default)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))
    }

    fn keyspace_exists(db: &Self::Db, name: &str) -> bool {
        db.db.keyspace_exists(name)
    }

    fn delete_keyspace(db: &Self::Db, partition: &Self::Partition) -> Result<(), MetaError> {
        // `delete_keyspace` lives on the plain database underneath and takes a
        // plain keyspace handle by value.
        db.db
            .inner()
            .delete_keyspace(partition.inner().clone())
            .map_err(|e| MetaError::OtherDBError(e.to_string()))
    }

    fn disk_space(db: &Self::Db) -> u64 {
        db.db.disk_space().unwrap_or(0)
    }

    // Upstream leaves `Store::num_keys` `unimplemented!()` on this backend,
    // which panics on the default `--metadata-db fjall` path. A read
    // transaction counts a keyspace fine -- it is the same call `len` uses.
    fn num_keys(db: &Self::Db, partition: &Self::Partition) -> Result<usize, MetaError> {
        Self::len(db, partition)
    }

    fn begin_transaction(store: &FjallStore) -> Transaction {
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
        //    `Arc<FjallStore>` cloned from `store`, and the store's database
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
                store.db().db.write_tx(),
            )
        };

        Transaction::new(Box::new(FjallTransaction::new(tx, Arc::new(store.clone()))))
    }

    fn get(partition: &Self::Partition, key: &[u8]) -> Result<Option<fjall::Slice>, MetaError> {
        partition
            .get(key)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))
    }

    fn insert(partition: &Self::Partition, key: &[u8], value: Vec<u8>) -> Result<(), MetaError> {
        partition
            .insert(key, value)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))
    }

    fn remove(partition: &Self::Partition, key: &[u8]) -> Result<(), MetaError> {
        partition
            .remove(key)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))
    }

    fn contains_key(partition: &Self::Partition, key: &[u8]) -> Result<bool, MetaError> {
        partition
            .contains_key(key)
            .map_err(|_| MetaError::KeyNotFound)
    }

    fn len(db: &Self::Db, partition: &Self::Partition) -> Result<usize, MetaError> {
        db.db
            .read_tx()
            .len(partition)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))
    }

    // The transactional keyspace handle has no iteration API of its own, so
    // reads go through a read transaction. The returned `Iter` owns its
    // snapshot nonce, so it stays valid after the read transaction is dropped.
    fn range<R: RangeBounds<Vec<u8>>>(
        db: &Self::Db,
        partition: &Self::Partition,
        range: R,
    ) -> fjall::Iter {
        db.db.read_tx().range::<Vec<u8>, _>(partition, range)
    }

    fn prefix(db: &Self::Db, partition: &Self::Partition, prefix: &[u8]) -> fjall::Iter {
        db.db.read_tx().prefix(partition, prefix)
    }
}

impl FjallStore {
    pub fn new(
        path: PathBuf,
        inlined_metadata_size: Option<usize>,
        durability: Option<Durability>,
    ) -> Self {
        tracing::debug!("Opening fjall store at {:?}", path);

        let db = fjall::SingleWriterTxDatabase::builder(&path)
            .open()
            .unwrap();

        // The mapping follows the POSIX names: fsync flushes data and
        // metadata (fjall SyncAll, strongest), fdatasync flushes data only
        // (fjall SyncData, weaker but faster). The default is the strongest
        // mode, same persist behavior as before the names were untangled.
        let durability = match durability.unwrap_or(Durability::Fsync) {
            Durability::Buffer => fjall::PersistMode::Buffer,
            Durability::Fsync => fjall::PersistMode::SyncAll,
            Durability::Fdatasync => fjall::PersistMode::SyncData,
        };

        FjallStoreOf::from_db(
            TxDb {
                db: Arc::new(db),
                durability,
            },
            inlined_metadata_size,
        )
    }
}

pub struct FjallTransaction {
    // FIELD ORDER IS LOAD-BEARING. DO NOT REORDER.
    //
    // `tx` holds a transaction whose lifetime was laundered to `'static` in
    // `Transactional::begin_transaction`; it borrows the database owned (via
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    crate::metastore::stores::test_utils::backend_test_battery!(FjallStore, || {
        let dir = tempdir().unwrap();
        let store = FjallStore::new(dir.path().to_path_buf(), Some(1), None);
        (store, dir)
    });
}
