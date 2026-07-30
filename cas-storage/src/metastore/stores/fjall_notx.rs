//! The non-transactional fjall backend.
//!
//! Everything that is not specific to the plain fjall database lives in
//! [`super::fjall_common`]; this module holds the flavor impl, the
//! rollback-tracking pseudo-transaction, and the constructor.

use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;

use fjall::{self, KeyspaceCreateOptions};

use crate::metastore::{MetaError, Transaction, TransactionBackend};

use super::fjall_common::{FjallFlavor, FjallStoreOf};

/// Metadata store backed by a plain (non-transactional) fjall database.
///
/// Writes land in the keyspace immediately; there is no write lock and no
/// engine-level rollback, so [`FjallNoTransaction`] undoes its own inserts
/// by hand. Weaker guarantees than [`super::fjall::FjallStore`], no
/// single-writer bottleneck.
pub type FjallStoreNotx = FjallStoreOf<NonTransactional>;

/// Flavor marker for the non-transactional backend. Never instantiated.
#[derive(Debug, Clone, Copy)]
pub struct NonTransactional;

impl FjallFlavor for NonTransactional {
    const STORE_NAME: &'static str = "FjallStoreNotx";
    const DB_LABEL: &'static str = "<fjall::Database>";

    type Db = Arc<fjall::Database>;
    type Partition = fjall::Keyspace;

    fn keyspace(db: &Self::Db, name: &str) -> Result<Self::Partition, MetaError> {
        db.keyspace(name, KeyspaceCreateOptions::default)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))
    }

    fn keyspace_exists(db: &Self::Db, name: &str) -> bool {
        db.keyspace_exists(name)
    }

    fn delete_keyspace(db: &Self::Db, partition: &Self::Partition) -> Result<(), MetaError> {
        db.delete_keyspace(partition.clone())
            .map_err(|e| MetaError::OtherDBError(e.to_string()))
    }

    fn disk_space(db: &Self::Db) -> u64 {
        db.disk_space().unwrap_or(0)
    }

    /// Approximate, unlike the transactional backend's exact count. Kept as
    /// upstream has it: this is the cheap call, and the exact one is
    /// available through `BaseMetaTree::len`.
    fn num_keys(_db: &Self::Db, partition: &Self::Partition) -> Result<usize, MetaError> {
        Ok(partition.approximate_len())
    }

    fn begin_transaction(store: &FjallStoreNotx) -> Transaction {
        Transaction::new(Box::new(FjallNoTransaction::new(Arc::new(store.clone()))))
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

    fn len(_db: &Self::Db, partition: &Self::Partition) -> Result<usize, MetaError> {
        partition
            .len()
            .map_err(|e| MetaError::OtherDBError(e.to_string()))
    }

    // The plain keyspace iterates directly; no read transaction involved.
    fn range<R: RangeBounds<Vec<u8>>>(
        _db: &Self::Db,
        partition: &Self::Partition,
        range: R,
    ) -> fjall::Iter {
        partition.range::<Vec<u8>, _>(range)
    }

    fn prefix(_db: &Self::Db, partition: &Self::Partition, prefix: &[u8]) -> fjall::Iter {
        partition.prefix(prefix)
    }
}

impl FjallStoreNotx {
    pub fn new(path: PathBuf, inlined_metadata_size: Option<usize>) -> Self {
        tracing::debug!("Opening fjall store at {:?}", path);

        let db = fjall::Database::builder(&path).open().unwrap();

        FjallStoreOf::from_db(Arc::new(db), inlined_metadata_size)
    }
}

/// The non-transactional stand-in for a write transaction: inserts go
/// straight to the keyspace and are remembered so that `rollback` can remove
/// them again. There is no isolation and no atomicity -- a crash mid-write
/// leaves the partial writes behind.
pub struct FjallNoTransaction {
    store: Arc<FjallStoreNotx>,

    inserted_keys: Vec<(String, Vec<u8>)>, // tupple of tree name and key
}

impl FjallNoTransaction {
    pub fn new(store: Arc<FjallStoreNotx>) -> Self {
        Self {
            store,
            inserted_keys: Vec::new(),
        }
    }
}

// Upstream had `unsafe impl Send`/`Sync` here. Both are redundant: the fields
// (`Arc<FjallStoreNotx>`, `Vec<(String, Vec<u8>)>`) are `Send + Sync`, so the
// auto impls apply. Deleted so the compiler notices if a future field changes
// that; the assertions pin the claim at the definition.
const _: () = {
    const fn assert_send<T: Send>() {}
    const fn assert_sync<T: Sync>() {}
    assert_send::<FjallNoTransaction>();
    assert_sync::<FjallNoTransaction>();
};

impl TransactionBackend for FjallNoTransaction {
    fn commit(&mut self) -> Result<(), MetaError> {
        Ok(())
    }

    fn rollback(&mut self) {
        for (tree_name, key) in &self.inserted_keys {
            let partition = self.store.get_partition(tree_name).unwrap();
            let _ = partition.remove(key);
        }
    }

    fn get(&mut self, tree_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        let partition = self.store.get_partition(tree_name)?;
        match partition.get(key) {
            Ok(Some(data)) => Ok(Some(data.to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    fn insert(&mut self, tree_name: &str, key: &[u8], data: Vec<u8>) -> Result<(), MetaError> {
        let partition = self.store.get_partition(tree_name)?;
        match partition.insert(key, data) {
            Ok(_) => {
                self.inserted_keys
                    .push((tree_name.to_string(), key.to_vec()));
                Ok(())
            }
            Err(e) => Err(MetaError::InsertError(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    crate::metastore::stores::test_utils::backend_test_battery!(FjallStoreNotx, || {
        let dir = tempdir().unwrap();
        let store = FjallStoreNotx::new(dir.path().to_path_buf(), Some(1));
        (store, dir)
    });
}
