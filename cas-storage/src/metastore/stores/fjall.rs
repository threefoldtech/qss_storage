use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::{convert::TryFrom, sync::Mutex};

use fjall::{self, KeyspaceCreateOptions, Readable, SingleWriterTxKeyspace};

use crate::metastore::{
    BaseMetaTree, Durability, KeyValuePairs, MetaError, MetaTreeExt, Object, Store, Transaction,
    TransactionBackend,
};

#[derive(Clone)]
pub struct FjallStore {
    db: Arc<fjall::SingleWriterTxDatabase>,
    inlined_metadata_size: usize,
    durability: fjall::PersistMode,
    partition_cache: Arc<Mutex<HashMap<String, Arc<SingleWriterTxKeyspace>>>>,
}

impl std::fmt::Debug for FjallStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallStore")
            .field("db", &"<fjall::SingleWriterTxDatabase>")
            .finish()
    }
}

const DEFAULT_INLINED_METADATA_SIZE: usize = 1; // setting very low will practically disable it by default

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
        let inlined_metadata_size = inlined_metadata_size.unwrap_or(DEFAULT_INLINED_METADATA_SIZE);

        let durability = durability.unwrap_or(Durability::Fdatasync);
        let durability = match durability {
            Durability::Buffer => fjall::PersistMode::Buffer,
            Durability::Fsync => fjall::PersistMode::SyncData,
            Durability::Fdatasync => fjall::PersistMode::SyncAll,
        };

        Self {
            db: Arc::new(db),
            inlined_metadata_size,
            durability,
            partition_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn get_partition(&self, name: &str) -> Result<Arc<SingleWriterTxKeyspace>, MetaError> {
        Ok(self
            .partition_cache
            .lock()
            .expect("Can lock partition cache")
            .entry(name.to_string())
            .or_insert_with(|| {
                Arc::new(
                    self.db
                        .keyspace(name, KeyspaceCreateOptions::default)
                        .expect("Can open keyspace"),
                )
            })
            .clone())
    }

    fn commit_persist(&self, tx: fjall::SingleWriterWriteTx) -> Result<(), MetaError> {
        tx.commit()
            .map_err(|e| MetaError::TransactionError(e.to_string()))?;

        self.db
            .persist(self.durability)
            .map_err(|e| MetaError::PersistError(e.to_string()))?;
        Ok(())
    }

    pub fn get_inlined_metadata_size(&self) -> usize {
        self.inlined_metadata_size
    }
}

impl Store for FjallStore {
    fn tree_open(&self, name: &str) -> Result<Arc<dyn BaseMetaTree>, MetaError> {
        let partition = self.get_partition(name)?;
        Ok(Arc::new(FjallTree::new(self.db.clone(), partition)))
    }

    fn tree_ext_open(&self, name: &str) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError> {
        let partition = self.get_partition(name)?;
        Ok(Arc::new(FjallTree::new(self.db.clone(), partition)))
    }

    fn tree_exists(&self, name: &str) -> Result<bool, MetaError> {
        Ok(self.db.keyspace_exists(name))
    }

    fn tree_delete(&self, name: &str) -> Result<(), MetaError> {
        let partition = self.get_partition(name)?;
        // Drop the cached handle so the inner Arc count goes to 1 path below;
        // delete_keyspace operates on a Keyspace handle by value.
        self.partition_cache
            .lock()
            .expect("Can lock partition cache")
            .remove(name);
        match self.db.inner().delete_keyspace(partition.inner().clone()) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    fn begin_transaction(&self) -> Transaction {
        tracing::debug!(target: "cas_storage::locks", "Transaction started");
        // Use unsafe to extend lifetime to 'static since the transaction
        // won't outlive the store
        let tx = unsafe {
            std::mem::transmute::<fjall::SingleWriterWriteTx<'_>, fjall::SingleWriterWriteTx<'static>>(
                self.db.write_tx(),
            )
        };

        Transaction::new(Box::new(FjallTransaction::new(tx, Arc::new(self.clone()))))
    }

    fn num_keys(&self, _: &str) -> Result<usize, MetaError> {
        unimplemented!("fjall with transaction does not support number of keys");
        // fjall with transaction does not support this
    }

    fn disk_space(&self) -> u64 {
        self.db.disk_space().unwrap_or(0)
    }
}

pub struct FjallTransaction {
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

unsafe impl Send for FjallTransaction {}
unsafe impl Sync for FjallTransaction {}

impl TransactionBackend for FjallTransaction {
    fn commit(&mut self) -> Result<(), MetaError> {
        if let Some(tx) = self.tx.take() {
            tracing::debug!(target: "cas_storage::locks", "Transaction commit started");
            let res = self.store.commit_persist(tx);
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

pub struct FjallTree {
    db: Arc<fjall::SingleWriterTxDatabase>,
    partition: Arc<SingleWriterTxKeyspace>,
}

impl FjallTree {
    pub fn new(
        db: Arc<fjall::SingleWriterTxDatabase>,
        partition: Arc<SingleWriterTxKeyspace>,
    ) -> Self {
        Self { db, partition }
    }

    fn get(&self, key: &[u8]) -> Result<Option<fjall::Slice>, MetaError> {
        match self.partition.get(key) {
            Ok(Some(v)) => Ok(Some(v)),
            Ok(None) => Ok(None),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }
}

impl BaseMetaTree for FjallTree {
    fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<(), MetaError> {
        match self.partition.insert(key, value) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    fn remove(&self, key: &[u8]) -> Result<(), MetaError> {
        match self.partition.remove(key) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    fn contains_key(&self, key: &[u8]) -> Result<bool, MetaError> {
        match self.partition.contains_key(key) {
            Ok(v) => Ok(v),
            Err(_) => Err(MetaError::KeyNotFound),
        }
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        match self.get(key) {
            Ok(Some(v)) => Ok(Some(v.to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    // ---- tfstor-extension: promoted out of #[cfg(test)] for runtime use ----
    fn len(&self) -> Result<usize, MetaError> {
        let read_tx = self.db.read_tx();
        let len = read_tx
            .len(&*self.partition)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))?;
        Ok(len)
    }
}

impl MetaTreeExt for FjallTree {
    fn iter_all(&self) -> KeyValuePairs {
        self.iter_kv(None)
    }

    // ---- tfstor-extension: BEGIN ----
    fn iter_kv(&self, start_after: Option<Vec<u8>>) -> KeyValuePairs {
        let partition = self.partition.clone();
        let db = self.db.clone();
        let mut last_key = start_after;

        Box::new(std::iter::from_fn(move || {
            let read_tx = db.read_tx();
            let range = match &last_key {
                Some(k) => {
                    let mut next = k.clone();
                    next.push(0);
                    next..
                }
                None => Vec::new()..,
            };

            read_tx
                .range::<Vec<u8>, _>(&*partition, range)
                .next()
                .map(|guard| match guard.into_inner() {
                    Ok((k, v)) => {
                        last_key = Some(k.to_vec());
                        Ok((k.to_vec(), v.to_vec()))
                    }
                    Err(e) => {
                        tracing::error!("Error reading key: {}", e);
                        Err(MetaError::OtherDBError(e.to_string()))
                    }
                })
        }))
    }

    fn iter_kv_backward(&self, start_key: Option<Vec<u8>>) -> KeyValuePairs {
        let partition = self.partition.clone();
        let db = self.db.clone();
        let mut last_key = start_key;

        Box::new(std::iter::from_fn(move || {
            let read_tx = db.read_tx();
            let range = match &last_key {
                Some(k) => ..k.clone(),
                None => ..Vec::new(),
            };

            read_tx
                .range::<Vec<u8>, _>(&*partition, range)
                .next_back()
                .map(|guard| match guard.into_inner() {
                    Ok((k, v)) => {
                        last_key = Some(k.to_vec());
                        Ok((k.to_vec(), v.to_vec()))
                    }
                    Err(e) => {
                        tracing::error!("Error reading key: {}", e);
                        Err(MetaError::OtherDBError(e.to_string()))
                    }
                })
        }))
    }
    // ---- tfstor-extension: END ----

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

        let read_tx = self.db.read_tx();

        let base_iter: Box<dyn Iterator<Item = fjall::Guard>> =
            match (prefix.as_ref(), ctsa.as_ref()) {
                (Some(prefix), Some(ctsa)) if (ctsa > prefix && !ctsa.starts_with(prefix)) => {
                    //Return empty iterator if ctsa is after prefix
                    Box::new(std::iter::empty())
                }
                (Some(prefix), Some(ctsa_local)) if ctsa_local < prefix => {
                    // If ctsa is before prefix, ignore ctsa
                    ctsa = None;
                    Box::new(read_tx.prefix(&*self.partition, prefix.as_bytes()))
                }
                (Some(prefix), _) => Box::new(read_tx.prefix(&*self.partition, prefix.as_bytes())),
                (None, Some(ctsa)) => {
                    let mut next_key = ctsa.as_bytes().to_vec();
                    next_key.push(0);
                    Box::new(read_tx.range(&*self.partition, next_key..))
                }
                (None, None) => Box::new(read_tx.range::<Vec<u8>, _>(&*self.partition, ..)),
            };

        let pairs = base_iter.filter_map(|g| g.into_inner().ok());

        let skip_filtered = if let (Some(_), Some(ctsa)) = (&prefix, ctsa) {
            let ctsa_bytes = ctsa.into_bytes();
            Box::new(pairs.skip_while(move |(raw_key, _)| &**raw_key <= ctsa_bytes.as_slice()))
                as Box<dyn Iterator<Item = _>>
        } else {
            Box::new(pairs)
        };

        Box::new(skip_filtered.map(|(raw_key, raw_value)| {
            let key = unsafe { String::from_utf8_unchecked(raw_key.to_vec()) };
            let obj = Object::try_from(&*raw_value).unwrap();
            (key, obj)
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::stores::test_utils;
    use tempfile::tempdir;

    fn setup_store() -> (FjallStore, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let store = FjallStore::new(dir.path().to_path_buf(), Some(1), None);
        (store, dir)
    }

    impl test_utils::TestStore for FjallStore {
        fn tree_open(&self, name: &str) -> Result<Arc<dyn BaseMetaTree>, MetaError> {
            <FjallStore as Store>::tree_open(self, name)
        }

        fn get_bucket_ext(
            &self,
            name: &str,
        ) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError> {
            <FjallStore as Store>::tree_ext_open(self, name)
        }
    }

    #[test]
    fn test_get_bucket_keys() {
        let (store, _dir) = setup_store();
        test_utils::test_get_bucket_keys(&store);
    }

    #[test]
    fn test_range_filter() {
        let (store, _dir) = setup_store();
        test_utils::test_range_filter(&store);
    }
}
