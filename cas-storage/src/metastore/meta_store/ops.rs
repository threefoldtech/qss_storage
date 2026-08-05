use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

use super::{
    BlockTree, DEFAULT_BLOCK_TREE, DEFAULT_BUCKET_TREE, DEFAULT_USAGE_TREE, MetaError, MetaStore,
    MetaTreeExt, Store, Transaction, decode_usage,
};
use crate::metastore::store_header::{self, HeaderSpec, StoreHeader, StoreHeaderError, StoreInit};
use crate::metastore::{BaseMetaTree, BucketMeta, Object};

impl MetaStore {
    /// Creates a new MetaStore instance with the given store implementation.
    ///
    /// This constructor neither writes nor checks the store header, so it will
    /// happily wrap a database of any vintage. Production code opens stores
    /// with [`MetaStore::open_or_create`] instead; this one stays for tests,
    /// benchmarks and tools that deliberately want the unvalidated view.
    ///
    /// # Arguments
    /// * `store` - The storage backend implementation
    /// * `inlined_metadata_size` - Optional size limit for inlined metadata. If None, a default value is used.
    ///
    /// # Returns
    /// A new MetaStore instance
    pub fn new(store: impl Store + 'static, inlined_metadata_size: Option<usize>) -> Self {
        const DEFAULT_INLINED_METADATA_SIZE: usize = 1; // setting very low will practically disable it by default

        Self {
            store: Arc::new(store),
            inlined_metadata_size: inlined_metadata_size.unwrap_or(DEFAULT_INLINED_METADATA_SIZE),
        }
    }

    /// Opens the store at `db_path`, creating it with a fresh QSST header if
    /// there is nothing there yet.
    ///
    /// `build` is handed the path and must open the backing database. It runs
    /// *after* the create-or-open decision has been made, which is the whole
    /// point of taking a closure: opening a fjall database creates the
    /// directory and its partitions, so once `build` has run the question
    /// "was there a store here?" can no longer be answered.
    ///
    /// The rule is: a path that does not exist, or an empty directory, is a
    /// new store and gets `spec`'s hash written into its header plus a sidecar
    /// copy next to the db directory. Anything else is an existing store, and
    /// its header decides -- `spec` is ignored, because its blocks are already
    /// addressed by whatever the header says.
    ///
    /// # Errors
    ///
    /// Whatever `build` returns -- [`MetaError::StoreLocked`] when another
    /// process holds the database, which is the routine case for an offline
    /// tool run against a live daemon.
    ///
    /// [`MetaError::Header`] if the store has no header (it predates the
    /// format), or one this build refuses: foreign magic, an unsupported
    /// version, or a hash it does not have. There is no fallback; see
    /// [`crate::metastore::store_header`].
    pub fn open_or_create<S: Store + 'static>(
        db_path: PathBuf,
        inlined_metadata_size: Option<usize>,
        spec: HeaderSpec,
        build: impl FnOnce(PathBuf) -> Result<S, MetaError>,
    ) -> Result<(Self, StoreHeader), MetaError> {
        let init = store_header::classify_db_dir(&db_path)?;

        let meta = Self::new(build(db_path.clone())?, inlined_metadata_size);

        let header = match init {
            StoreInit::Create => {
                let header =
                    StoreHeader::create(spec).map_err(|e| MetaError::header(&db_path, e))?;
                store_header::write_header(&*meta.store, &header)?;
                // The sidecar belongs in the STORE directory, which on this
                // path is always the database's parent: every store this
                // build CREATES has its database in a subdirectory of the
                // directory the operator named. A store whose database is
                // that directory (respcas before ADR 0014) is only ever
                // opened, and the one write that rewrites its sidecar is
                // told where to put it -- `store_header::raise_version`.
                match db_path.parent() {
                    Some(store_dir) => store_header::write_sidecar(store_dir, &header),
                    None => tracing::warn!(
                        "no parent directory for {}: store header sidecar not written",
                        db_path.display()
                    ),
                }
                tracing::debug!(
                    "created QSST store at {} (version {}, algo {}, width {})",
                    db_path.display(),
                    header.version(),
                    header.hash_algo(),
                    header.hash_width()
                );
                header
            }
            StoreInit::Open => store_header::read_header(&*meta.store, &db_path)?
                .ok_or_else(|| MetaError::header(&db_path, StoreHeaderError::Missing))?,
        };

        Ok((meta, header))
    }

    /// Returns the maximum length of the data that can be inlined in the metadata object.
    ///
    /// Inlining small data directly in metadata can improve performance by reducing the number
    /// of storage operations needed for small objects.
    ///
    /// # Returns
    /// The maximum number of bytes that can be inlined
    pub fn max_inlined_data_length(&self) -> usize {
        if self.inlined_metadata_size < Object::minimum_inline_metadata_size() {
            return 0;
        }
        self.inlined_metadata_size - Object::minimum_inline_metadata_size()
    }

    /// Returns a reference to the underlying store.
    ///
    /// This is used for creating additional stores that share the same storage backend,
    /// such as UserStore in multi-user mode.
    ///
    /// # Returns
    /// An Arc reference to the underlying Store implementation
    pub fn get_underlying_store(&self) -> Arc<dyn Store> {
        Arc::clone(&self.store)
    }

    /// Returns the tree which contains all the buckets.
    ///
    /// This tree is used to store the bucket lists and provide
    /// the CRUD operations for the bucket list.
    ///
    /// # Returns
    /// A tree with extended functionality for bucket operations or an error
    pub fn get_allbuckets_tree(&self) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError> {
        self.store.tree_ext_open(DEFAULT_BUCKET_TREE)
    }

    /// Returns the tree for a specific bucket with extended methods.
    ///
    /// This tree provides additional methods for the bucket like range queries and listing operations.
    ///
    /// # Arguments
    /// * `name` - The name of the bucket
    ///
    /// # Returns
    /// A tree with extended functionality for the specified bucket or an error
    pub fn get_bucket_ext(
        &self,
        name: &str,
    ) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError> {
        self.store.tree_ext_open(name)
    }

    /// Returns the block metadata tree.
    ///
    /// This tree is used to store the data block metadata, including reference counts
    /// and other block-specific information.
    ///
    /// # Returns
    /// A BlockTree instance or an error
    pub fn get_block_tree(&self) -> Result<BlockTree, MetaError> {
        let tree = self.store.tree_ext_open(DEFAULT_BLOCK_TREE)?;
        Ok(BlockTree { tree })
    }

    /// Returns a tree with the given name.
    ///
    /// This is typically used when the application needs to store custom metadata
    /// for a specific purpose outside the standard bucket/object model.
    ///
    /// # Arguments
    /// * `name` - The name of the tree to open
    ///
    /// # Returns
    /// A tree instance or an error
    pub fn get_tree(&self, name: &str) -> Result<Arc<dyn BaseMetaTree>, MetaError> {
        self.store.tree_open(name)
    }

    /// Returns a tree with the given name and the extended surface, which is
    /// what `iter_all` lives on.
    ///
    /// [`Self::get_bucket_ext`] is the same call under a name that says
    /// "bucket"; this one is for the trees that are not buckets, such as
    /// [`super::MULTIPART_PARTS_TREE`].
    ///
    /// # Arguments
    /// * `name` - The name of the tree to open
    ///
    /// # Returns
    /// A tree with extended functionality or an error
    pub fn get_tree_ext(
        &self,
        name: &str,
    ) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError> {
        self.store.tree_ext_open(name)
    }

    /// Returns the name of every tree in the store, reserved `_`-prefixed
    /// trees included.
    ///
    /// The store itself is the authority on which trees exist. ADR 0005's
    /// holder enumeration starts here rather than at [`Self::list_buckets`],
    /// because a bucket whose teardown crashed after its `_BUCKETS` row was
    /// removed still has an object tree, and that tree still holds block
    /// references.
    ///
    /// # Returns
    /// Every tree name, in no particular order, or an error
    pub fn list_trees(&self) -> Result<Vec<String>, MetaError> {
        self.store.list_trees()
    }

    /// Checks if a bucket with the given name exists.
    ///
    /// # Arguments
    /// * `bucket_name` - The name of the bucket to check
    ///
    /// # Returns
    /// `true` if the bucket exists, `false` otherwise, or an error
    pub fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError> {
        self.store.tree_exists(bucket_name)
    }

    /// Deletes the bucket with the given name: its object tree AND its row in
    /// `_BUCKETS`.
    ///
    /// Both, because the name is what [`insert_bucket_if_absent`](Self::insert_bucket_if_absent)
    /// claims: a dropped bucket whose row survived would be a name nobody
    /// could take again, which is what respcas's FLUSH does -- drop the
    /// namespace and create it back. Callers that remove the row themselves
    /// first (`bucket_delete`, and fsck resuming a crashed teardown) are
    /// unaffected: removing an absent key is not an error.
    ///
    /// If the bucket doesn't exist, this operation is a no-op and returns success.
    ///
    /// # Arguments
    /// * `name` - The name of the bucket to delete
    ///
    /// # Returns
    /// Success or an error if the deletion fails
    pub fn drop_bucket(&self, name: &str) -> Result<(), MetaError> {
        self.store
            .tree_open(DEFAULT_BUCKET_TREE)?
            .remove(name.as_bytes())?;
        // The usage counter goes with the records it counted. A bucket
        // recreated under the same name starts from nothing, which is what
        // respcas's FLUSH means by emptying a namespace.
        self.store
            .tree_open(DEFAULT_USAGE_TREE)?
            .remove(name.as_bytes())?;
        if self.bucket_exists(name)? {
            self.store.tree_delete(name)
        } else {
            Ok(())
        }
    }

    /// Logical bytes the records of `bucket` add up to, or `None` if the
    /// bucket has no counter.
    ///
    /// LOGICAL: the sum of the `size` field of every object record, which is
    /// what a client stored -- not what it costs on disk, which dedup and
    /// block sharing make a property of the store rather than of a bucket.
    /// respcas's namespace quota is spent in these bytes (`max_size`), and
    /// two namespaces holding one deduplicated value are each charged for it,
    /// because either of them can be the one that keeps it alive.
    ///
    /// The counter is maintained inside the same transaction as the record
    /// mutation that moves it ([`Transaction::add_bucket_usage`]), so it
    /// cannot drift from the records by crashing between the two.
    ///
    /// `None` means "never accounted", which is what a bucket written before
    /// the counter existed says -- distinct from `Some(0)`, an empty bucket
    /// that is being counted. A caller that cares (respcas, adopting a store
    /// into the quota ledger) sums the records itself and writes the total
    /// with [`set_bucket_usage`](Self::set_bucket_usage).
    pub fn bucket_usage(&self, bucket: &str) -> Result<Option<u64>, MetaError> {
        match self
            .store
            .tree_open(DEFAULT_USAGE_TREE)?
            .get(bucket.as_bytes())?
        {
            Some(raw) => decode_usage(bucket, &raw).map(Some),
            None => Ok(None),
        }
    }

    /// Writes `bucket`'s usage counter outright.
    ///
    /// For the one caller that derives the number from the records rather
    /// than from a delta: the adoption of a bucket that predates the counter.
    /// Everything else moves it by a delta inside the transaction that moves
    /// the records ([`Transaction::add_bucket_usage`]).
    pub fn set_bucket_usage(&self, bucket: &str, bytes: u64) -> Result<(), MetaError> {
        self.store
            .tree_open(DEFAULT_USAGE_TREE)?
            .insert(bucket.as_bytes(), bytes.to_le_bytes().to_vec())
    }

    /// Inserts a raw representation of a bucket into the meta store.
    ///
    /// This method both adds the bucket metadata to the buckets tree and
    /// creates the bucket's own tree if it doesn't already exist.
    ///
    /// # Arguments
    /// * `bucket_name` - The name of the bucket
    /// * `raw_bucket` - The serialized bucket metadata
    ///
    /// # Returns
    /// Success or an error if the insertion fails
    ///
    /// # Errors
    ///
    /// [`MetaError::ReservedBucketName`] for a name starting with `_`. A
    /// bucket becomes a tree of its own name, so such a bucket would collide
    /// with the store's internal trees (`_STORE_HEADER`, `_BUCKETS`,
    /// `_BLOCKS`, `_MULTIPART_PARTS`, `_UPLOADS`) and hand a client the store's
    /// own bookkeeping. S3 bucket naming forbids these names anyway; this is
    /// the store enforcing it for every caller, respcas included.
    pub fn insert_bucket(&self, bucket_name: &str, raw_bucket: Vec<u8>) -> Result<(), MetaError> {
        if bucket_name.starts_with('_') {
            return Err(MetaError::ReservedBucketName(bucket_name.to_string()));
        }

        // Insert the bucket metadata into the buckets tree
        let buckets = self.store.tree_open(DEFAULT_BUCKET_TREE)?;
        buckets.insert(bucket_name.as_bytes(), raw_bucket)?;

        // Create the bucket tree if it doesn't exist
        self.store.tree_open(bucket_name)?;

        // A bucket this build created is accounted from its first byte, so
        // its counter reads zero rather than absent -- which is reserved for
        // "written before there was a counter". See [`Self::bucket_usage`].
        self.set_bucket_usage(bucket_name, 0)?;

        Ok(())
    }

    /// Inserts a bucket record only if the name is free, answering whether it
    /// did.
    ///
    /// [`insert_bucket`](Self::insert_bucket)'s racing sibling, for a caller
    /// that has to be able to tell "I created it" from "somebody else did":
    /// respcas's NSNEW, which is a command with an answer. Checking with
    /// [`bucket_exists`](Self::bucket_exists) and then inserting is two steps,
    /// and two callers can both pass the check -- so both are told they
    /// created the namespace, and the second one's default record overwrites
    /// whatever the first (or an NSSET in between) had already put there.
    ///
    /// The read and the insert are one transaction on `_BUCKETS`, which under
    /// fjall's single writer makes exactly one caller the winner. The bucket's
    /// own tree is created after the commit, so a namespace is never
    /// configurable before its record exists.
    ///
    /// # Errors
    ///
    /// [`MetaError::ReservedBucketName`] for a `_`-prefixed name, as
    /// [`insert_bucket`](Self::insert_bucket), and refused before anything is
    /// written.
    pub fn insert_bucket_if_absent(
        &self,
        bucket_name: &str,
        raw_bucket: Vec<u8>,
    ) -> Result<bool, MetaError> {
        if bucket_name.starts_with('_') {
            return Err(MetaError::ReservedBucketName(bucket_name.to_string()));
        }

        let mut tx = self.begin_transaction();
        let taken = match tx.get_bucket_record(bucket_name) {
            Ok(record) => record.is_some(),
            Err(e) => {
                tx.rollback();
                return Err(e);
            }
        };
        if taken {
            tx.rollback();
            return Ok(false);
        }
        // The record and the bucket's usage counter are claimed together: a
        // bucket this build created is accounted from its first byte, and its
        // counter reads zero rather than absent (see [`Self::bucket_usage`]).
        let claimed = tx
            .put_bucket_record(bucket_name, raw_bucket)
            .and_then(|()| tx.reset_bucket_usage(bucket_name));
        if let Err(e) = claimed {
            tx.rollback();
            return Err(e);
        }
        tx.commit()?;

        // Create the bucket tree if it doesn't exist
        self.store.tree_open(bucket_name)?;

        Ok(true)
    }

    /// Returns a list of all buckets in the system.
    ///
    /// # Returns
    /// A vector of BucketMeta objects or an error
    ///
    /// # Note
    /// This method currently loads all buckets into memory at once.
    /// TODO: This should be paginated and return a stream for better scalability.
    ///
    /// A record that fails to decode fails the whole listing: silently dropping
    /// it would report a bucket as gone while its tree and data are still there.
    pub fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError> {
        let bucket = self.get_allbuckets_tree()?;
        bucket
            .iter_all()
            .map(|result| {
                let (_, value) = result?;
                // Just return the BucketMeta without the key
                BucketMeta::try_from(&*value).map_err(MetaError::from)
            })
            .collect()
    }

    /// The name of every bucket in the store, taken from the `_BUCKETS`
    /// KEYS rather than from the records under them.
    ///
    /// The key is the name -- [`insert_bucket`](Self::insert_bucket) files
    /// each record under it -- so this answers "which buckets exist" without
    /// depending on what the value happens to be. That matters because the
    /// value is not always a [`BucketMeta`]: respcas stores its own
    /// namespace metadata there (msgpack, with the key mode ADR 0014 added),
    /// and a walker that decoded it would fail on a healthy store.
    ///
    /// Every caller that only wants names uses this; `list_buckets` stays
    /// for the S3 listing, which needs the creation time in the record.
    pub fn list_bucket_names(&self) -> Result<Vec<String>, MetaError> {
        let bucket = self.get_allbuckets_tree()?;
        bucket
            .iter_all()
            .map(|result| {
                let (key, _) = result?;
                String::from_utf8(key).map_err(|e| {
                    MetaError::OtherDBError(format!("bucket name is not valid utf-8: {e}"))
                })
            })
            .collect()
    }

    /// Inserts a metadata Object into the specified bucket.
    ///
    /// # Arguments
    /// * `bucket_name` - The name of the bucket
    /// * `key` - The key to associate with the object
    /// * `raw_obj` - The serialized object metadata
    ///
    /// # Returns
    /// Success or an error if the insertion fails
    pub fn insert_meta(
        &self,
        bucket_name: &str,
        key: &str,
        raw_obj: Vec<u8>,
    ) -> Result<(), MetaError> {
        let bucket = self.get_bucket_ext(bucket_name)?;
        bucket.insert(key.as_bytes(), raw_obj)
    }

    /// Retrieves the Object metadata for the given bucket and key.
    ///
    /// This method returns the deserialized Object struct instead of raw bytes
    /// for better performance and convenience.
    ///
    /// # Arguments
    /// * `bucket_name` - The name of the bucket
    /// * `key` - The key to look up, as bytes: an S3 key is text, but a
    ///   respcas Cas namespace keys its records by the raw BLAKE3 of the
    ///   value (ADR 0014), which is not.
    ///
    /// # Returns
    /// The Object if found, None if the key doesn't exist, or an error
    pub fn get_meta(&self, bucket_name: &str, key: &[u8]) -> Result<Option<Object>, MetaError> {
        let bucket = self.get_bucket_ext(bucket_name)?;
        match bucket.get(key)? {
            Some(data) => {
                let obj = Object::try_from(&*data)?;
                Ok(Some(obj))
            }
            None => Ok(None),
        }
    }

    /// Begins a new transaction for atomic operations.
    ///
    /// # Returns
    /// A new Transaction object
    pub fn begin_transaction(&self) -> Transaction {
        self.store.begin_transaction()
    }

    // ---- tfstor-extension: BEGIN ----
    /// Returns the total number of keys in the bucket tree.
    ///
    /// This is primarily used for monitoring and debugging purposes.
    ///
    /// # Returns
    /// The number of keys in the bucket tree, or the backend error that
    /// prevented counting them.
    ///
    /// Upstream returns a bare `usize` and `unwrap()`s the store call, turning
    /// any backend error into a panic in a debugging helper.
    pub fn num_keys(&self) -> Result<usize, MetaError> {
        self.store.num_keys(DEFAULT_BUCKET_TREE)
    }
    // ---- tfstor-extension: END ----

    /// Returns the total disk space used by the metadata store.
    ///
    /// # Returns
    /// The disk space usage in bytes
    pub fn disk_space(&self) -> u64 {
        self.store.disk_space()
    }
}

impl Debug for MetaStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaStore")
            .field("store", &"<Store>")
            .field("bucket_tree_name", &DEFAULT_BUCKET_TREE)
            .field("block_tree_name", &DEFAULT_BLOCK_TREE)
            .field("inlined_metadata_size", &self.inlined_metadata_size)
            .finish()
    }
}
