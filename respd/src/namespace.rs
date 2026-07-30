use std::collections::HashMap;
use std::sync::RwLock;
use std::{convert::TryFrom, sync::Arc};

use anyhow::Result;
use bytes::Bytes;
use md5::{Digest, Md5};
use tracing::debug;

use crate::storage::{Storage, StorageError};
use cas_storage::{MetaError, MetaTreeExt, Object, ObjectData};

/// Properties for a namespace
#[derive(Debug, Clone)]
pub struct NamespaceProperties {
    /// Name of the namespace this properties belongs to
    pub namespace_name: String,
    /// Write Once Read Many mode - if true, keys can only be written once and never modified or deleted
    pub worm: bool,
    /// Locked mode - if true, no set or delete operations are allowed
    pub locked: bool,
    /// Public mode - if false and password is set, authentication is required for read operations
    pub public: bool,
}

impl Default for NamespaceProperties {
    fn default() -> Self {
        Self {
            namespace_name: "default".to_string(),
            worm: false,
            locked: false,
            public: true, // Default to public access
        }
    }
}

/// Represents a namespace with its associated tree
pub struct Namespace {
    /// The tree for this namespace
    pub tree: RwLock<Arc<dyn MetaTreeExt + Send + Sync>>,
    /// Properties for this namespace
    pub properties: RwLock<NamespaceProperties>,
}

/// A cache for namespace instances to allow sharing between clients
pub struct NamespaceCache {
    storage: Arc<Storage>,
    namespaces: RwLock<HashMap<String, Arc<Namespace>>>,
}

impl NamespaceCache {
    /// Create a new namespace cache
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            storage,
            namespaces: RwLock::new(HashMap::new()),
        }
    }

    /// Get a namespace from the cache or existing storage
    pub fn get_or_create(&self, name: String) -> Result<Arc<Namespace>, StorageError> {
        // First, try to get from cache
        {
            let namespaces = self.namespaces.read().unwrap();
            if let Some(namespace) = namespaces.get(&name) {
                debug!("Using cached namespace: {}", name);
                return Ok(namespace.clone());
            }
        }

        // Not in cache, try to get from storage
        let tree = self.storage.get_namespace(name.as_str())?;

        // Namespace exists in storage, create a new namespace object
        debug!(
            "Creating new namespace object for existing namespace: {}",
            name
        );
        let props = NamespaceProperties {
            namespace_name: name.clone(),
            ..Default::default()
        };
        let namespace = Arc::new(Namespace {
            tree: RwLock::new(tree),
            properties: RwLock::new(props),
        });

        // Sync properties with metadata
        if let Ok(meta) = self.storage.get_namespace_meta(&name) {
            let _ = namespace.sync_properties_from_meta(&meta);
        }

        // Store in cache
        {
            let mut namespaces = self.namespaces.write().unwrap();
            namespaces.insert(name, namespace.clone());
        }

        Ok(namespace)
    }

    /// Update all instances of a namespace in the cache
    /// This ensures that all clients using this namespace will see the updated properties
    pub fn update_all_instances<F>(&self, name: &str, update_fn: F)
    where
        F: Fn(&Arc<Namespace>),
    {
        // Get all instances from cache
        let namespaces = self.namespaces.read().unwrap();
        if let Some(namespace) = namespaces.get(name) {
            // Apply the update function to the namespace
            update_fn(namespace);
            debug!("Updated namespace instance in cache: {}", name);
        } else {
            debug!("Namespace not found in cache, no update needed: {}", name);
        }
    }

    /// Create a namespace if it doesn't exist and return it
    pub fn create_if_not_exists(&self, name: String) -> Result<Arc<Namespace>, StorageError> {
        match self.get_or_create(name.clone()) {
            Ok(namespace) => Ok(namespace),
            Err(_) => {
                // Namespace doesn't exist in storage, create a new one
                debug!("Creating new namespace in storage: {}", name);
                let tree = self.storage.create_namespace(&name)?;
                let props = NamespaceProperties {
                    namespace_name: name.clone(),
                    ..Default::default()
                };
                let namespace = Arc::new(Namespace {
                    tree: RwLock::new(tree),
                    properties: RwLock::new(props),
                });

                // Sync properties with metadata
                if let Ok(meta) = self.storage.get_namespace_meta(&name) {
                    let _ = namespace.sync_properties_from_meta(&meta);
                }

                // Store in cache
                {
                    let mut namespaces = self.namespaces.write().unwrap();
                    namespaces.insert(name, namespace.clone());
                }

                Ok(namespace)
            }
        }
    }

    /// Flush a namespace by dropping and recreating its bucket
    /// This operation will clear all keys in the namespace
    pub fn flush_namespace(&self, name: &str) -> Result<(), StorageError> {
        // Write lock the cache to prevent concurrent access during flush
        let namespaces_lock = self.namespaces.read().unwrap();

        // Get the namespace from the cache
        if let Some(namespace) = namespaces_lock.get(name) {
            // Get current namespace metadata before dropping the bucket
            let namespace_meta = self.storage.get_namespace_meta(name)?;

            // Drop the old tree by replacing it with a new one
            {
                let placeholder_tree = self.storage.get_namespace("default")?;

                // Get a write lock on the tree
                let mut tree_lock = namespace.tree.write().unwrap();

                // Replace the old tree with the placeholder
                *tree_lock = placeholder_tree;

                // The lock will be dropped at the end of this scope, releasing the placeholder tree
            }

            // Drop the bucket from storage
            self.storage.delete_namespace(name)?;

            // Create a new namespace with the same name
            let new_tree = self.storage.create_namespace(name)?;

            // Assign the new tree to the namespace
            let mut tree_lock = namespace.tree.write().unwrap();
            *tree_lock = new_tree;

            // Restore the original metadata to preserve properties
            self.storage.update_namespace_meta(name, namespace_meta)?;
        }

        debug!("Flushed namespace: {}", name);
        Ok(())
    }
}

impl Namespace {
    /// Sync properties with the persistent metadata
    /// This is called when the namespace is loaded to ensure in-memory properties
    /// reflect the persistent metadata
    pub fn sync_properties_from_meta(
        &self,
        meta: &crate::storage::NamespaceMeta,
    ) -> Result<(), MetaError> {
        let mut props = self.properties.write().unwrap();
        props.worm = meta.worm;
        props.locked = meta.locked;
        props.public = meta.public;
        Ok(())
    }

    pub fn flush(&self, namespace_cache: &NamespaceCache) -> Result<()> {
        // Get the namespace name
        let namespace_name = self.properties.read().unwrap().namespace_name.clone();

        // Check if this is the default namespace
        if namespace_name == "default" {
            return Err(anyhow::anyhow!("ERR: Cannot flush the default namespace"));
        }

        namespace_cache.flush_namespace(&namespace_name)?;
        Ok(())
    }

    pub fn set(&self, key: &[u8], value: Bytes) -> Result<()> {
        // Read namespace properties
        let props = self.properties.read().unwrap();

        // Check if namespace is locked
        if props.locked {
            return Err(anyhow::anyhow!(
                "ERR: Namespace is temporarily locked (read-only)"
            ));
        }

        // Check if namespace is in WORM mode and key already exists
        if props.worm {
            // In WORM mode, check if key already exists
            if self.exists(key)? {
                return Err(anyhow::anyhow!("ERR: Namespace is protected by worm mode"));
            }
        }

        // Note: Authentication check is now handled by the CommandHandler

        // Proceed with setting the key
        let data = value.to_vec();
        let hash = Md5::digest(&data).into();
        let size = data.len() as u64;
        let obj_meta = Object::new(size, hash, ObjectData::Inline { data });
        self.tree.read().unwrap().insert(key, obj_meta.to_vec())?;
        Ok(())
    }

    /// Get an Object from the tree for a given key
    fn get_object(&self, key: &[u8]) -> Result<Option<Object>, MetaError> {
        match self.tree.read().unwrap().get(key)? {
            Some(data) => {
                let obj = Object::try_from(&*data).expect("Malformed object");
                Ok(Some(obj))
            }
            None => Ok(None),
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>, MetaError> {
        let obj_meta = self.get_object(key)?;
        match obj_meta {
            Some(obj) => {
                if let Some(data) = obj.inlined() {
                    let bytes = bytes::Bytes::from(data.clone());
                    Ok(Some(bytes))
                } else {
                    Err(MetaError::OtherDBError("Object is not inline".to_string()))
                }
            }
            None => Ok(None),
        }
    }

    pub fn del(&self, key: &[u8]) -> Result<()> {
        // Read namespace properties
        let props = self.properties.read().unwrap();

        // Check if namespace is locked
        if props.locked {
            return Err(anyhow::anyhow!(
                "ERR: Namespace is temporarily locked (read-only)"
            ));
        }

        // Check if namespace is in WORM mode
        if props.worm {
            return Err(anyhow::anyhow!(
                "ERR: Cannot delete a key when namespace is in worm mode"
            ));
        }

        // Note: Authentication check is now handled by the CommandHandler

        // Proceed with deleting the key
        self.tree.read().unwrap().remove(key)?;
        Ok(())
    }

    pub fn exists(&self, key: &[u8]) -> Result<bool, MetaError> {
        self.tree.read().unwrap().contains_key(key)
    }

    /// Get the length (size) of a key's value
    /// Returns None if the key doesn't exist
    pub fn length(&self, key: &[u8]) -> Result<Option<u64>, MetaError> {
        match self.get_object(key)? {
            Some(obj) => Ok(Some(obj.size())),
            None => Ok(None),
        }
    }

    /// Get the last-modified timestamp of a key
    /// Returns None if the key doesn't exist
    pub fn keytime(&self, key: &[u8]) -> Result<Option<i64>, MetaError> {
        match self.get_object(key)? {
            Some(obj) => {
                // Get the last modified time as Unix timestamp (seconds since epoch)
                let system_time = obj.last_modified();
                let timestamp = system_time
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;
                Ok(Some(timestamp))
            }
            None => Ok(None),
        }
    }

    pub fn check(&self, key: &[u8]) -> Result<Option<bool>, MetaError> {
        let obj = self.get_object(key)?;
        match obj {
            Some(obj) => {
                if let Some(data) = obj.inlined() {
                    // check the hash
                    let hash: [u8; 16] = Md5::digest(data).into();
                    if hash != *obj.hash() {
                        Ok(Some(false))
                    } else {
                        Ok(Some(true))
                    }
                } else {
                    Err(MetaError::OtherDBError("Object is not inline".to_string()))
                }
            }
            None => Ok(None),
        }
    }

    pub fn num_keys(&self) -> Result<usize, MetaError> {
        self.tree.read().unwrap().len()
    }

    pub fn scan(
        &self,
        start_key: Option<Vec<u8>>,
        num_keys: u32,
    ) -> Result<Vec<Vec<u8>>, MetaError> {
        let mut keys = Vec::new();
        let mut count = 0;

        for result in self.tree.read().unwrap().iter_kv(start_key) {
            match result {
                Ok((key, _)) => {
                    keys.push(key);
                    count += 1;
                    if count >= num_keys {
                        break;
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Ok(keys)
    }

    pub fn scan_backward(
        &self,
        start_key: Option<Vec<u8>>,
        num_keys: u32,
    ) -> Result<Vec<Vec<u8>>, MetaError> {
        let mut keys = Vec::new();
        let mut count = 0;

        for result in self.tree.read().unwrap().iter_kv_backward(start_key) {
            match result {
                Ok((key, _)) => {
                    keys.push(key);
                    count += 1;
                    if count >= num_keys {
                        break;
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Ok(keys)
    }
}
