use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::info;

use cas_storage::{Durability, FjallStore, HeaderSpec, MetaError, MetaStore, MetaTreeExt};

// Default tree name for key-value storage
// No longer using a default tree name as we'll use the namespace as the tree name

/// Storage implementation using metastore with inlined data
pub struct Storage {
    store: MetaStore,
}

impl Storage {
    /// Create a new MetaStorage instance, or open the one already at
    /// `data_dir`.
    ///
    /// respd never addresses a block, but its DB carries the same QSST header
    /// as every other store in this workspace: that is what gives it format
    /// versioning, and what makes a store from before the format refuse to
    /// open instead of being read as garbage.
    ///
    /// # Errors
    ///
    /// [`MetaError::Header`] if the directory holds a store this build will
    /// not open; the message names the store and the reason.
    pub fn new(data_dir: PathBuf, inlined_metadata_size: Option<usize>) -> Result<Self, MetaError> {
        // Create the metastore with FjallStore backend
        // Strongest persist mode (POSIX fsync semantics: data + metadata);
        // same behavior as before the Durability naming was untangled.
        let (store, _header) = MetaStore::open_or_create(
            data_dir,
            inlined_metadata_size,
            HeaderSpec::default(),
            |path| FjallStore::new(path, inlined_metadata_size, Some(Durability::Fsync)),
        )?;

        Ok(Self { store })
    }

    /// Initialize the default namespace if it doesn't exist
    pub fn init_namespace(&self) -> Result<(), StorageError> {
        let default_namespace = "default";

        // Check if the default namespace exists
        if !self.store.bucket_exists(default_namespace)? {
            info!("Default namespace not found, creating it");
            // Create the default namespace
            self.create_namespace(default_namespace)?;
        }

        Ok(())
    }

    /// Get a namespace instance for a specific namespace name
    pub fn get_namespace(
        &self,
        name: &str,
    ) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, StorageError> {
        if !self.store.bucket_exists(name)? {
            return Err(StorageError::NamespaceNotFound);
        }

        // TODO: get namespace meta

        self.store
            .get_bucket_ext(name)
            .map_err(|e| StorageError::MetaError(e.to_string()))
    }

    pub fn get_namespace_meta(&self, name: &str) -> Result<NamespaceMeta, StorageError> {
        if !self.store.bucket_exists(name)? {
            return Err(StorageError::NamespaceNotFound);
        }

        let bucketlist_tree = self.store.get_allbuckets_tree()?;
        let raw = bucketlist_tree
            .get(name.as_bytes())
            .map_err(|e| StorageError::MetaError(e.to_string()))?;
        if let Some(raw) = raw {
            NamespaceMeta::from_msgpack(&raw).map_err(|e| StorageError::MetaError(e.to_string()))
        } else {
            Err(StorageError::NamespaceNotFound)
        }
    }

    /// Update the metadata for a namespace
    pub fn update_namespace_meta(
        &self,
        name: &str,
        meta: NamespaceMeta,
    ) -> Result<(), StorageError> {
        if !self.store.bucket_exists(name)? {
            return Err(StorageError::NamespaceNotFound);
        }

        let meta_raw = meta
            .to_msgpack()
            .map_err(|e| MetaError::OtherDBError(e.to_string()))?;

        let bucketlist_tree = self.store.get_allbuckets_tree()?;
        bucketlist_tree
            .insert(name.as_bytes(), meta_raw)
            .map_err(|e| StorageError::MetaError(e.to_string()))?;
        Ok(())
    }

    pub fn create_namespace(
        &self,
        name: &str,
    ) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, StorageError> {
        if self.store.bucket_exists(name)? {
            return Err(StorageError::NamespaceNotFound);
        }

        let namespace_meta_raw = NamespaceMeta::new(name.to_string())
            .to_msgpack()
            .map_err(|e| MetaError::OtherDBError(e.to_string()))?;

        self.store.insert_bucket(name, namespace_meta_raw)?;

        self.get_namespace(name)
    }

    pub fn delete_namespace(&self, name: &str) -> Result<(), StorageError> {
        if !self.store.bucket_exists(name)? {
            return Err(StorageError::NamespaceNotFound);
        }

        self.store.drop_bucket(name)?;
        Ok(())
    }

    /// Iterate over all namespaces in the storage
    ///
    /// Returns an iterator that yields NamespaceMeta structs for each namespace
    pub fn iter_namespace(
        &self,
    ) -> Result<impl Iterator<Item = Result<NamespaceMeta, StorageError>>, StorageError> {
        // Get the all buckets tree which contains namespace metadata
        let bucketlist_tree = self.store.get_allbuckets_tree()?;

        // Use tree.iter_kv to iterate over all key-value pairs in the tree
        let kv_pairs = bucketlist_tree.iter_kv(None);

        // Transform the iterator to yield NamespaceMeta structs
        let namespace_iter = kv_pairs.map(|kv_result| {
            kv_result
                .map_err(|e| StorageError::MetaError(e.to_string()))
                .and_then(|(_key, value)| {
                    // Create NamespaceMeta struct from the value using from_msgpack
                    NamespaceMeta::from_msgpack(&value)
                        .map_err(|e| StorageError::MetaError(e.to_string()))
                })
        });

        Ok(namespace_iter)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NamespaceMeta {
    pub name: String,
    pub password: Option<String>,
    pub max_size: Option<u64>,
    pub public: bool,
    pub worm: bool,
    pub locked: bool,
    pub key_mode: KeyMode,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum KeyMode {
    UserKey,
    Sequential,
}

impl NamespaceMeta {
    /// Create a new NamespaceMeta with default values
    pub fn new(name: String) -> Self {
        Self {
            name,
            password: None,
            max_size: None,
            public: true,
            worm: false,
            locked: false,
            key_mode: KeyMode::UserKey,
        }
    }

    /// Encode the NamespaceMeta to MessagePack format
    pub fn to_msgpack(&self) -> Result<Vec<u8>> {
        rmp_serde::to_vec(self)
            .map_err(|e| anyhow::anyhow!("Failed to encode NamespaceMeta to MessagePack: {}", e))
    }

    /// Decode a NamespaceMeta from MessagePack format
    pub fn from_msgpack(data: &[u8]) -> Result<Self> {
        rmp_serde::from_slice(data)
            .map_err(|e| anyhow::anyhow!("Failed to decode NamespaceMeta from MessagePack: {}", e))
    }
}

#[derive(Debug)]
pub enum StorageError {
    NamespaceNotFound,
    MetaError(String),
}

// Implement the std::error::Error trait
impl Error for StorageError {}

// Implement the Display trait for custom error messages
impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            StorageError::NamespaceNotFound => write!(f, "Namespace not found"),
            StorageError::MetaError(ref msg) => write!(f, "{}", msg),
        }
    }
}

// Implement conversion from MetaError to StorageError
impl From<MetaError> for StorageError {
    fn from(error: MetaError) -> Self {
        StorageError::MetaError(error.to_string())
    }
}
