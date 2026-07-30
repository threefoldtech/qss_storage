use std::convert::TryFrom;
use std::path::PathBuf;
use std::sync::Arc;

use fjall::{self, KeyspaceCreateOptions};

use crate::metastore::{
    BaseMetaTree, KeyValuePairs, MetaError, MetaTreeExt, Object, Store, Transaction,
    TransactionBackend,
};

#[derive(Clone)]
pub struct FjallStoreNotx {
    db: Arc<fjall::Database>,
    inlined_metadata_size: usize,
}

impl std::fmt::Debug for FjallStoreNotx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallStoreNotx")
            .field("db", &"<fjall::Database>")
            .finish()
    }
}

impl FjallStoreNotx {
    pub fn new(path: PathBuf, inlined_metadata_size: Option<usize>) -> Self {
        tracing::debug!("Opening fjall store at {:?}", path);

        let db = fjall::Database::builder(&path).open().unwrap();
        // setting very low will practically disable it by default
        let inlined_metadata_size = inlined_metadata_size.unwrap_or(1);

        Self {
            db: Arc::new(db),
            inlined_metadata_size,
        }
    }

    fn get_partition(&self, name: &str) -> Result<fjall::Keyspace, MetaError> {
        match self.db.keyspace(name, KeyspaceCreateOptions::default) {
            Ok(partition) => Ok(partition),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    pub fn get_inlined_metadata_size(&self) -> usize {
        self.inlined_metadata_size
    }
}

impl Store for FjallStoreNotx {
    fn tree_open(&self, name: &str) -> Result<Arc<dyn BaseMetaTree>, MetaError> {
        let partition = self.get_partition(name)?;
        Ok(Arc::new(FjallTreeNotx::new(Arc::new(partition))))
    }

    fn tree_ext_open(&self, name: &str) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError> {
        let partition = self.get_partition(name)?;
        Ok(Arc::new(FjallTreeNotx::new(Arc::new(partition))))
    }

    fn tree_exists(&self, name: &str) -> Result<bool, MetaError> {
        Ok(self.db.keyspace_exists(name))
    }

    fn tree_delete(&self, name: &str) -> Result<(), MetaError> {
        let partition = self.get_partition(name)?;
        match self.db.delete_keyspace(partition) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    fn begin_transaction(&self) -> Transaction {
        Transaction::new(Box::new(FjallNoTransaction::new(Arc::new(self.clone()))))
    }

    fn num_keys(&self, tree_name: &str) -> Result<usize, MetaError> {
        let partition = self.get_partition(tree_name)?;
        Ok(partition.approximate_len())
    }

    fn disk_space(&self) -> u64 {
        self.db.disk_space().unwrap_or(0)
    }
}

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

// ---- tfstor-extension: BEGIN ----
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
// ---- tfstor-extension: END ----

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

pub struct FjallTreeNotx {
    partition: Arc<fjall::Keyspace>,
}

impl FjallTreeNotx {
    pub fn new(partition: Arc<fjall::Keyspace>) -> Self {
        Self { partition }
    }

    fn get(&self, key: &[u8]) -> Result<Option<fjall::Slice>, MetaError> {
        match self.partition.get(key) {
            Ok(Some(v)) => Ok(Some(v)),
            Ok(None) => Ok(None),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }
}

impl BaseMetaTree for FjallTreeNotx {
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
        let len = self
            .partition
            .len()
            .map_err(|e| MetaError::OtherDBError(e.to_string()))?;
        Ok(len)
    }
}

impl MetaTreeExt for FjallTreeNotx {
    fn iter_all(&self) -> KeyValuePairs {
        self.iter_kv(None)
    }

    // ---- tfstor-extension: BEGIN ----
    fn iter_kv(&self, start_after: Option<Vec<u8>>) -> KeyValuePairs {
        let partition = self.partition.clone();
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

            partition
                .range::<Vec<u8>, _>(range)
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
        let mut last_key = start_key;

        Box::new(std::iter::from_fn(move || {
            let range = match &last_key {
                Some(k) => ..k.clone(),
                None => ..Vec::new(),
            };

            partition
                .range::<Vec<u8>, _>(range)
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

        let partition = self.partition.clone();

        let base_iter: Box<dyn Iterator<Item = fjall::Guard>> =
            match (prefix.as_ref(), ctsa.as_ref()) {
                (Some(prefix), Some(ctsa)) if (ctsa > prefix && !ctsa.starts_with(prefix)) => {
                    //Return empty iterator if ctsa is after prefix
                    Box::new(std::iter::empty())
                }
                (Some(prefix), Some(ctsa_local)) if ctsa_local < prefix => {
                    // If ctsa is before prefix, ignore ctsa
                    ctsa = None;
                    Box::new(partition.prefix(prefix.as_bytes()))
                }
                (Some(prefix), _) => Box::new(partition.prefix(prefix.as_bytes())),
                (None, Some(ctsa)) => {
                    let mut next_key = ctsa.as_bytes().to_vec();
                    next_key.push(0);
                    Box::new(partition.range(next_key..))
                }
                (None, None) => Box::new(partition.range::<Vec<u8>, _>(..)),
            };

        let pairs = base_iter.filter_map(|g| g.into_inner().ok());

        let skip_filtered = if let (Some(_), Some(ctsa)) = (&prefix, ctsa) {
            let ctsa_bytes = ctsa.into_bytes();
            Box::new(pairs.skip_while(move |(raw_key, _)| &**raw_key <= ctsa_bytes.as_slice()))
                as Box<dyn Iterator<Item = _>>
        } else {
            Box::new(pairs)
        };

        // ---- tfstor-extension: BEGIN ----
        // See the matching comment in `stores/fjall.rs`: the key comes off disk,
        // so it is validated rather than assumed. `range_filter` has an
        // infallible item type, so an invalid key is logged and skipped.
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
        // ---- tfstor-extension: END ----
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::stores::test_utils;
    use tempfile::tempdir;

    impl test_utils::TestStore for FjallStoreNotx {
        fn tree_open(&self, name: &str) -> Result<Arc<dyn BaseMetaTree>, MetaError> {
            <FjallStoreNotx as Store>::tree_open(self, name)
        }

        fn get_bucket_ext(
            &self,
            name: &str,
        ) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError> {
            <FjallStoreNotx as Store>::tree_ext_open(self, name)
        }

        fn num_keys(&self, name: &str) -> Result<usize, MetaError> {
            <FjallStoreNotx as Store>::num_keys(self, name)
        }
    }

    fn setup_store() -> (FjallStoreNotx, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let store = FjallStoreNotx::new(dir.path().to_path_buf(), Some(1));
        (store, dir)
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

    // ---- tfstor-extension: BEGIN ----
    #[test]
    fn test_num_keys() {
        let (store, _dir) = setup_store();
        test_utils::test_num_keys(&store);
    }
    // ---- tfstor-extension: END ----
}
