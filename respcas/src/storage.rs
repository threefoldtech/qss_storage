use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::info;

use cas_storage::metastore::store_header::{self, STORE_HEADER_VERSION_CAS_NAMESPACE};
use cas_storage::{Durability, FjallStore, HeaderSpec, MetaError, MetaStore, MetaTreeExt};

// Default tree name for key-value storage
// No longer using a default tree name as we'll use the namespace as the tree name

/// Storage implementation using metastore with inlined data
pub struct Storage {
    store: MetaStore,
    /// The metadata database's own directory: what the QSST header belongs
    /// to, and what a header error names.
    db_path: PathBuf,
}

impl Storage {
    /// Create a new MetaStorage instance, or open the one already at
    /// `data_dir`.
    ///
    /// respcas never addresses a block, but its DB carries the same QSST header
    /// as every other store in this workspace: that is what gives it format
    /// versioning, and what makes a store from before the format refuse to
    /// open instead of being read as garbage.
    ///
    /// `durability` and `header` come from the config file merge in `main`;
    /// `header` applies only if the store is created now, since an existing
    /// one is opened on the header it already carries.
    ///
    /// # Errors
    ///
    /// [`MetaError::Header`] if the directory holds a store this build will
    /// not open; the message names the store and the reason.
    ///
    /// [`MetaError::StoreLocked`] if another process already has the data
    /// directory open -- a second respcas on the same `--data-dir`. Both errors
    /// reach `main`, which reports them and exits nonzero rather than
    /// panicking.
    pub fn new(
        data_dir: PathBuf,
        inlined_metadata_size: Option<usize>,
        durability: Durability,
        header: HeaderSpec,
    ) -> Result<Self, MetaError> {
        // Create the metastore with FjallStore backend
        let (store, _header) =
            MetaStore::open_or_create(data_dir.clone(), inlined_metadata_size, header, |path| {
                FjallStore::new(path, inlined_metadata_size, Some(durability))
            })?;

        Ok(Self {
            store,
            db_path: data_dir,
        })
    }

    /// Records in the store's header that it now holds a content-addressed
    /// namespace (ADR 0014).
    ///
    /// Called BEFORE the first `key_mode = cas` namespace metadata is
    /// written, and that order is the whole point: the metadata carries a
    /// msgpack variant an older build cannot decode, and the raised header is
    /// what makes such a build refuse the store at the open instead of
    /// failing mid-flight over a record it cannot read.
    ///
    /// Idempotent -- a store already at the raised version is left alone --
    /// so it costs a point read on every call after the first.
    fn declare_cas_namespaces(&self) -> Result<(), StorageError> {
        store_header::raise_version(
            &*self.store.get_underlying_store(),
            &self.db_path,
            STORE_HEADER_VERSION_CAS_NAMESPACE,
        )?;
        Ok(())
    }

    /// Changes a namespace's key mode, which is permitted only while the
    /// namespace holds no keys (ADR 0014).
    ///
    /// A populated namespace cannot switch: its existing keys would stop
    /// meaning what they say -- user keys are not hashes, and hashes are not
    /// names -- so the refusal is the honest answer rather than a migration
    /// nobody asked for.
    ///
    /// Switching TO [`KeyMode::Cas`] raises the store's header first (see
    /// [`Storage::declare_cas_namespaces`]); switching away from it leaves
    /// the raised header where it is, because the store has held such a
    /// namespace and older builds must keep refusing it.
    pub fn set_key_mode(&self, name: &str, key_mode: KeyMode) -> Result<(), StorageError> {
        let mut meta = self.get_namespace_meta(name)?;
        if meta.key_mode == key_mode {
            return Ok(());
        }

        let keys = self.get_namespace(name)?.len()?;
        if keys != 0 {
            return Err(StorageError::NamespaceNotEmpty {
                namespace: name.to_string(),
                keys,
            });
        }

        if key_mode == KeyMode::Cas {
            self.declare_cas_namespaces()?;
        }

        meta.key_mode = key_mode;
        self.update_namespace_meta(name, meta)
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

/// How a namespace decides what a record's key is.
///
/// Serialized inside [`NamespaceMeta`] as a msgpack variant NAME, so the
/// order of these does not matter and an added variant does not renumber the
/// others -- but an older build meeting `Cas` fails its decode, which is why
/// a store gains a Cas namespace only together with the QSST header raise
/// (ADR 0014, `STORE_HEADER_VERSION_CAS_NAMESPACE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyMode {
    /// The client names the key and the server stores whatever it is told:
    /// the zdb-heritage default, and what every namespace was until ADR
    /// 0014.
    UserKey,
    /// zdb heritage, vestigial here: the server would assign an increasing
    /// key. Nothing implements it; it survives because it is on disk in
    /// stores that were created with it.
    Sequential,
    /// The key of every record IS the BLAKE3-256 hash of its value -- 32 raw
    /// bytes (ADR 0014). Immutable per key by construction: a SET of a key
    /// that is present is a dedup hit, and DEL is the only real mutation.
    Cas,
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
    /// A change that is only coherent on an empty namespace was asked for on
    /// one that holds keys (ADR 0014's key-mode gate).
    NamespaceNotEmpty {
        namespace: String,
        keys: usize,
    },
    MetaError(String),
}

// Implement the std::error::Error trait
impl Error for StorageError {}

// Implement the Display trait for custom error messages
impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            StorageError::NamespaceNotFound => write!(f, "Namespace not found"),
            StorageError::NamespaceNotEmpty {
                ref namespace,
                keys,
            } => write!(
                f,
                "namespace {namespace} holds {keys} key(s): the key mode can only be \
                 changed while a namespace is empty"
            ),
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

/// A namespace metadata record as it was serialized before ADR 0014 added
/// the `Cas` key mode -- and what happens to it now.
///
/// The msgpack encoding of a fieldless enum variant is its NAME, so adding a
/// variant renumbers nothing and an old record decodes untouched. That is the
/// assumption the ADR names as a known unknown, and this is where it is
/// pinned: if it ever stops holding, an explicit meta migration is owed, and
/// these tests are what would say so.
#[cfg(test)]
mod meta_compatibility {
    use super::*;

    /// The byte-for-byte record a pre-0014 build wrote for a default
    /// namespace. Encoded as a struct (an array of field values), with
    /// `key_mode` as the string "UserKey".
    ///
    /// Changing this vector changes what respcas stores on disk.
    const PRE_0014_DEFAULT: &[u8] = &[
        0x97, // array of 7 fields
        0xa7, b'd', b'e', b'f', b'a', b'u', b'l', b't', // name: "default"
        0xc0, // password: nil
        0xc0, // max_size: nil
        0xc3, // public: true
        0xc2, // worm: false
        0xc2, // locked: false
        0xa7, b'U', b's', b'e', b'r', b'K', b'e', b'y', // key_mode: "UserKey"
    ];

    #[test]
    fn a_pre_0014_record_decodes_unchanged() {
        let meta = NamespaceMeta::from_msgpack(PRE_0014_DEFAULT)
            .expect("a record written before the Cas variant must still decode");

        assert_eq!(meta.name, "default");
        assert_eq!(meta.password, None);
        assert_eq!(meta.max_size, None);
        assert!(meta.public);
        assert!(!meta.worm);
        assert!(!meta.locked);
        assert_eq!(meta.key_mode, KeyMode::UserKey);

        // And this build re-encodes it to the same bytes: adding a variant
        // moved nothing, so a store round-tripped through it is unchanged.
        assert_eq!(meta.to_msgpack().unwrap(), PRE_0014_DEFAULT);
    }

    #[test]
    fn every_key_mode_round_trips() {
        for mode in [KeyMode::UserKey, KeyMode::Sequential, KeyMode::Cas] {
            let mut meta = NamespaceMeta::new("ns".to_string());
            meta.key_mode = mode;
            let raw = meta.to_msgpack().unwrap();
            assert_eq!(NamespaceMeta::from_msgpack(&raw).unwrap().key_mode, mode);
        }
    }

    /// The other direction of the compatibility gate, spelled out: the Cas
    /// variant is written as a name an older build's decoder has no arm for,
    /// which is why the store's header is raised before such a record is
    /// ever written (`Storage::declare_cas_namespaces`).
    #[test]
    fn the_cas_variant_is_a_name_an_older_decoder_has_no_arm_for() {
        let mut meta = NamespaceMeta::new("archive".to_string());
        meta.key_mode = KeyMode::Cas;
        let raw = meta.to_msgpack().unwrap();

        assert!(
            raw.windows(3).any(|w| w == b"Cas"),
            "the variant travels as its name: {raw:?}"
        );

        // The pre-0014 shape of the type, as far as serde is concerned.
        #[derive(Debug, Deserialize)]
        enum OldKeyMode {
            #[allow(dead_code)]
            UserKey,
            #[allow(dead_code)]
            Sequential,
        }
        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        struct OldMeta {
            name: String,
            password: Option<String>,
            max_size: Option<u64>,
            public: bool,
            worm: bool,
            locked: bool,
            key_mode: OldKeyMode,
        }

        assert!(
            rmp_serde::from_slice::<OldMeta>(&raw).is_err(),
            "an older build fails this decode; the header raise is what stops it \
             ever reaching one"
        );
    }
}
