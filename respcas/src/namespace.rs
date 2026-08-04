use std::collections::HashMap;
use std::sync::RwLock;
use std::{convert::TryFrom, sync::Arc};

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, stream};
use md5::{Digest, Md5};
use tracing::debug;

use crate::storage::{KeyMode, Storage, StorageError};
use cas_storage::{
    AsyncByteStream, BlockStream, CasFS, ContentHash, MetaError, MetaTreeExt, Object, ObjectData,
    RangeRequest, SharedMetrics,
};

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
    /// What a key means here (ADR 0014): a name the client chose, or the
    /// BLAKE3-256 of the value. The in-memory copy of the persisted
    /// `key_mode`, kept here because it decides what SET does and every
    /// command reads it.
    pub key_mode: KeyMode,
}

impl Default for NamespaceProperties {
    fn default() -> Self {
        Self {
            namespace_name: "default".to_string(),
            worm: false,
            locked: false,
            public: true, // Default to public access
            key_mode: KeyMode::UserKey,
        }
    }
}

/// Represents a namespace with its associated tree
pub struct Namespace {
    /// The tree for this namespace
    pub tree: RwLock<Arc<dyn MetaTreeExt + Send + Sync>>,
    /// Properties for this namespace
    pub properties: RwLock<NamespaceProperties>,
    /// The block engine, for the paths that are not a single record: the
    /// ADR 0006 write path, the block-backed read, and the rc release a
    /// DELETE owes (ADR 0014). A namespace is a bucket to it.
    cas: CasFS,
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
            cas: self.storage.cas().clone(),
        });

        // Sync properties with metadata
        if let Ok(meta) = self.storage.get_namespace_meta(&name) {
            namespace.sync_properties_from_meta(&meta);
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
                    cas: self.storage.cas().clone(),
                });

                // Sync properties with metadata
                if let Ok(meta) = self.storage.get_namespace_meta(&name) {
                    namespace.sync_properties_from_meta(&meta);
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
    pub fn sync_properties_from_meta(&self, meta: &crate::storage::NamespaceMeta) {
        let mut props = self.properties.write().unwrap();
        props.worm = meta.worm;
        props.locked = meta.locked;
        props.public = meta.public;
        props.key_mode = meta.key_mode;
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

    /// This namespace's name, which is also its bucket name in the store.
    pub fn name(&self) -> String {
        self.properties.read().unwrap().namespace_name.clone()
    }

    /// What a key means here (ADR 0014).
    pub fn key_mode(&self) -> KeyMode {
        self.properties.read().unwrap().key_mode
    }

    /// Refuses the write if the namespace is not accepting one.
    ///
    /// `worm_guards_key` is the key a WORM namespace must not already hold.
    /// `None` skips that check, which is what a content-addressed write
    /// passes: its "overwrite" cannot change a byte -- the key IS the
    /// content -- so an already-present address is an acknowledgement rather
    /// than a modification, and refusing it would break the probe-then-store
    /// workflow WORM exists to make safe.
    fn refuse_unless_writable(&self, worm_guards_key: Option<&[u8]>) -> Result<()> {
        let props = self.properties.read().unwrap();

        if props.locked {
            return Err(anyhow::anyhow!(
                "ERR: Namespace is temporarily locked (read-only)"
            ));
        }

        if props.worm {
            // The lookup is inside the branch: an ordinary write must not pay
            // a read to find out that the namespace it is writing to is not
            // write-once.
            let occupied = match worm_guards_key {
                Some(key) => self.exists(key)?,
                None => false,
            };
            if occupied {
                return Err(anyhow::anyhow!("ERR: Namespace is protected by worm mode"));
            }
        }

        Ok(())
    }

    pub fn set(&self, key: &[u8], value: Bytes) -> Result<()> {
        self.refuse_unless_writable(Some(key))?;

        // Note: Authentication check is now handled by the CommandHandler

        // Proceed with setting the key. A user-keyed namespace inlines every
        // value whatever its size, exactly as respcas always has: the block
        // path is the content-addressed namespaces' (ADR 0014), and giving
        // it to this one would change the durability and latency of writes
        // no client asked to change.
        let data = value.to_vec();
        let hash = ContentHash(Md5::digest(&data).into());
        let size = data.len() as u64;
        let obj_meta = Object::new(size, hash, ObjectData::Inline { data });
        self.tree.read().unwrap().insert(key, obj_meta.to_vec())?;
        Ok(())
    }

    /// Writes `value` under `key` in a content-addressed namespace, where
    /// `key` is known to be the BLAKE3-256 of `value` (ADR 0014).
    ///
    /// Values at or below the inline threshold stay in their own record,
    /// exactly as a user-keyed write does; above it the value goes through
    /// the ADR 0006 block write path and the record names blocks. The
    /// threshold is the store's (`store.inline_metadata_size`), so one knob
    /// governs both faces of the workspace.
    ///
    /// Only [`crate::content::ingest`] may call this: it is the code that
    /// has established that the key is the value's address.
    pub async fn store_verified(&self, key: &[u8], value: Bytes) -> Result<()> {
        self.refuse_unless_writable(None)?;

        let bucket = self.name();
        if value.len() <= self.cas.max_inlined_data_length() {
            self.cas
                .store_inlined_object(&bucket, key, value.to_vec())
                .await?;
            return Ok(());
        }

        let stream = AsyncByteStream::new(stream::once(async move { Ok(value) }));
        let (blocks, hash, size) = self.cas.store_object(&bucket, key, stream).await?;
        // The ETag field of the shared record envelope. In a
        // content-addressed namespace it carries no authority -- the key is
        // the stronger digest -- but the envelope has the field and every
        // other writer fills it in.
        self.cas
            .create_object_meta(&bucket, key, size, hash, ObjectData::SinglePart { blocks })
            .await?;
        Ok(())
    }

    /// Get an Object from the tree for a given key
    fn get_object(&self, key: &[u8]) -> Result<Option<Object>, MetaError> {
        match self.tree.read().unwrap().get(key)? {
            Some(data) => {
                let obj = Object::try_from(&*data)?;
                Ok(Some(obj))
            }
            None => Ok(None),
        }
    }

    /// The whole value of `key`, or `None` if there is no record.
    ///
    /// An inline record answers from its own bytes; a block-backed one
    /// (which only a content-addressed namespace has, ADR 0014) is read from
    /// the block files and concatenated, because a RESP bulk reply is a
    /// length-prefixed whole and there is nothing to stream it into.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, MetaError> {
        let Some(obj) = self.get_object(key)? else {
            return Ok(None);
        };
        if let Some(data) = obj.inlined() {
            return Ok(Some(Bytes::from(data.clone())));
        }
        Ok(Some(self.read_blocks(key, &obj).await?))
    }

    /// Reads a block-backed record's value whole.
    ///
    /// Verification of the blocks against their own addresses is the read
    /// path's (`store.verify_on_read`); what this adds is nothing, on
    /// purpose -- CHECK is where a caller asks for the value to be re-hashed
    /// against its key.
    async fn read_blocks(&self, key: &[u8], obj: &Object) -> Result<Bytes, MetaError> {
        let bucket = self.name();
        let Some((_, paths)) = self.cas.get_object_paths(&bucket, key)? else {
            return Err(MetaError::OtherDBError(format!(
                "the record for {} went away while it was being read",
                crate::content::hex(key)
            )));
        };

        let size = obj.size() as usize;
        let mut stream = BlockStream::new(paths, size, RangeRequest::All, SharedMetrics::default());
        if self.cas.verify_on_read() {
            stream = stream.verified(self.cas.hasher(), obj.blocks().to_vec());
        }

        let mut out = BytesMut::with_capacity(size);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| {
                MetaError::OtherDBError(format!(
                    "reading the blocks of {}: {e}",
                    crate::content::hex(key)
                ))
            })?;
            out.extend_from_slice(&chunk);
        }
        Ok(out.freeze())
    }

    /// Deletes a key, answering whether it existed -- the unit DEL counts.
    ///
    /// A block-backed record's references are dropped as part of the delete
    /// (ADR 0008's release path), so the blocks of a value nothing else
    /// holds are freed here and not by a sweep.
    pub async fn del(&self, key: &[u8]) -> Result<bool> {
        {
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
        }

        // Note: Authentication check is now handled by the CommandHandler

        // Proceed with deleting the key. The record is taken and its blocks
        // released in the one operation, so the reply counts what this call
        // removed rather than what a separate lookup saw a moment earlier.
        let bucket = self.name();
        Ok(self.cas.delete_object(&bucket, key).await?)
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

    /// Re-hashes a record's value and says whether it still matches.
    ///
    /// What it is checked against depends on what the key means (ADR 0014):
    ///
    /// - user-keyed: the MD5 the record carries, which is what CHECK has
    ///   always compared;
    /// - content-addressed: the KEY, which is the BLAKE3-256 of the value --
    ///   a strictly stronger check, and the only one that can be made
    ///   against a block-backed record whose bytes are not in the record.
    ///
    /// `None` means there is no such key. Cost is O(size) either way, the
    /// same class as GET.
    pub async fn check(&self, key: &[u8]) -> Result<Option<bool>, MetaError> {
        let Some(obj) = self.get_object(key)? else {
            return Ok(None);
        };

        if self.key_mode() == KeyMode::Cas {
            let value = match obj.inlined() {
                Some(data) => Bytes::from(data.clone()),
                None => self.read_blocks(key, &obj).await?,
            };
            return Ok(Some(crate::content::value_key(&value) == key));
        }

        match obj.inlined() {
            Some(data) => Ok(Some(ContentHash(Md5::digest(data).into()) == *obj.hash())),
            None => Err(MetaError::OtherDBError("Object is not inline".to_string())),
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
