mod storage_engine;

use std::sync::Arc;
use std::{io, path::PathBuf};

use super::multipart::{MultiPart, part_key};
use super::shared_block_store::SharedBlockStore;
use super::uploads::UploadClaim;
use crate::metrics::SharedMetrics;

use crate::metastore::{
    BlockId, BlockTree, BucketMeta, ContentHash, FjallStore, HeaderSpec, MetaError, MetaStore,
    MetaTreeExt, Object, ObjectData, UploadRecord,
};
use crate::store_options::StoreOptions;

use super::byte_stream::AsyncByteStream;

pub use storage_engine::StorageEngine;

pub const BLOCK_SIZE: usize = 1 << 20; // Supposedly 1 MiB

/// One namespace's view of a store: its own metadata DB, plus the block store
/// it shares with every sibling namespace.
///
/// A clone shares the same `SharedBlockStore` `Arc` -- one stripe set, one
/// blocks DB, one blocks root -- so ADR 0006's one-store-one-instance rule
/// survives cloning; that is what lets the stale-upload GC hold its own handle
/// beside the S3 service (ADR 0003).
#[derive(Clone)]
pub struct CasFS {
    pub(super) namespace: MetaStore,
    pub(super) shared: Arc<SharedBlockStore>,
    pub(super) metrics: SharedMetrics,
    pub(super) verify_on_read: bool,
}

pub type ObjectPaths = (Object, Vec<(PathBuf, usize)>);

impl CasFS {
    /// Build a `CasFS` for one namespace, sharing a block/multipart store
    /// across namespaces via `shared`.
    ///
    /// Layout on disk:
    ///   `namespace_meta_path/db/` - this namespace's metadata DB
    ///   (the shared DB and the block data files live wherever
    ///   `SharedBlockStore::new` was given -- the blocks root is the store's,
    ///   not this namespace's, so cross-namespace dedup always resolves to
    ///   one set of files)
    ///
    /// The namespace DB is headered like every other store. Its header takes
    /// the hash the shared block store already carries, so the two DBs of one
    /// deployment can never disagree about how blocks are addressed.
    ///
    /// # Errors
    ///
    /// [`MetaError::Header`] if the namespace DB exists but its header is
    /// missing or unacceptable; see [`MetaStore::open_or_create`].
    ///
    /// [`MetaError::StoreLocked`] if another process already holds the
    /// namespace DB. Routine rather than exceptional -- it is what an offline
    /// tool meets when the daemon is running -- so it is a value, not a panic.
    ///
    /// `opts` says how this process opens the store; the only field that
    /// applies to a namespace DB and not to the block store is
    /// [`StoreOptions::verify_on_read`], and `opts.hasher` is ignored here
    /// because the namespace header takes the hash the block store already
    /// carries.
    pub fn new(
        mut namespace_meta_path: PathBuf,
        shared: Arc<SharedBlockStore>,
        metrics: SharedMetrics,
        opts: StoreOptions,
    ) -> Result<Self, MetaError> {
        namespace_meta_path.push("db");

        // Canonicalize to eliminate getcwd() syscalls in async operations
        std::fs::create_dir_all(&namespace_meta_path).ok();
        namespace_meta_path = namespace_meta_path
            .canonicalize()
            .unwrap_or(namespace_meta_path);

        let spec = HeaderSpec::from(shared.hasher());
        let inlined_metadata_size = opts.inline_metadata_size;
        let (namespace, _header) = match opts.metadata_db {
            StorageEngine::Fjall => {
                MetaStore::open_or_create(namespace_meta_path, inlined_metadata_size, spec, |p| {
                    FjallStore::new(p, inlined_metadata_size, Some(opts.durability))
                })?
            }
        };

        Ok(Self {
            namespace,
            shared,
            metrics,
            verify_on_read: opts.verify_on_read,
        })
    }

    /// Convenience constructor for single-namespace consumers (CLI ops,
    /// tests, third-party library users who only need one namespace).
    ///
    /// Builds a dedicated `SharedBlockStore` with its blocks DB at
    /// `meta_path/blocks/.db/` and its block data files at `root/blocks/`,
    /// and returns a `CasFS` whose namespace metadata lives at
    /// `meta_path/db/`.
    ///
    /// Two headered DBs are involved: the blocks DB, whose header names the
    /// block hash, and the namespace DB, which inherits it. `opts.hasher`
    /// applies only to DBs that are created now.
    ///
    /// `opts` is passed whole to both constructors; see [`StoreOptions`] for
    /// what each knob does and which ADR put it there.
    ///
    /// # One process, one store
    ///
    /// Every call mints a PRIVATE `SharedBlockStore` -- with its own stripe
    /// set and placement state. A process must not open the same on-disk
    /// store through this twice: two instances would not share stripes, and
    /// the per-block serialization that the write and delete protocols rely
    /// on (ADR 0006) would silently not hold between them. Open one `CasFS`
    /// per store, or build one `SharedBlockStore` and hand it to
    /// [`CasFS::new`] per namespace.
    pub fn single_namespace(
        root: PathBuf,
        meta_path: PathBuf,
        metrics: SharedMetrics,
        opts: StoreOptions,
    ) -> Result<Self, MetaError> {
        let shared = Arc::new(SharedBlockStore::new(
            meta_path.join("blocks"),
            root.join("blocks"),
            opts,
        )?);
        Self::new(meta_path, shared, metrics, opts)
    }

    /// Build a `CasFS` over a namespace metadata store that is already open.
    ///
    /// The seam respcas needs (ADR 0014). Its store is one `MetaStore` whose
    /// buckets are namespaces, opened by respcas itself because its data
    /// directory has a layout of its own (and a legacy shape to keep
    /// opening), so it cannot go through [`CasFS::new`] -- which would open a
    /// second database beside the one it already holds. What it needs is the
    /// block engine bolted onto the store it has.
    ///
    /// `namespace` must be the store's OWN metadata database -- the one
    /// holding `_BUCKETS` and the per-bucket object trees -- and `shared` the
    /// block store paired with it. Nothing here can check that pairing, which
    /// is why every other caller goes through the constructors that establish
    /// it.
    pub fn over_namespace(
        namespace: MetaStore,
        shared: Arc<SharedBlockStore>,
        metrics: SharedMetrics,
        verify_on_read: bool,
    ) -> Self {
        Self {
            namespace,
            shared,
            metrics,
            verify_on_read,
        }
    }

    /// The hash function this filesystem addresses blocks with, taken from the
    /// block store's header at open.
    pub fn hasher(&self) -> crate::hasher::Hasher {
        self.shared.hasher()
    }

    /// Whether reads re-hash each block and refuse to serve one whose bytes no
    /// longer hash to its address. Off by default: it costs a full buffer plus
    /// a hash per block.
    ///
    /// # Limitation
    ///
    /// Whole blocks only. A range request is served from the same block files
    /// without verification, because a partial block cannot be re-hashed
    /// against a whole-block address. Callers that need the guarantee must ask
    /// for the whole object.
    pub fn verify_on_read(&self) -> bool {
        self.verify_on_read
    }

    /// Root directory of the block data files -- the store's, shared by
    /// every namespace.
    pub fn fs_root(&self) -> &PathBuf {
        self.shared.blocks_root()
    }

    /// This namespace's own metadata store: the DB holding its bucket trees.
    ///
    /// Distinct from the shared block store's metadata (`_BLOCKS`,
    /// `_MULTIPART_PARTS`), which lives in a different database. Tools that
    /// walk the namespace's holders -- fsck (ADR 0005) -- start here.
    pub fn namespace_meta_store(&self) -> &MetaStore {
        &self.namespace
    }

    /// The block store this namespace shares with its siblings.
    pub fn shared_block_store(&self) -> &Arc<SharedBlockStore> {
        &self.shared
    }

    pub fn max_inlined_data_length(&self) -> usize {
        self.namespace.max_inlined_data_length()
    }

    pub fn get_bucket(
        &self,
        bucket_name: &str,
    ) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError> {
        super::buckets::get_bucket(self, bucket_name)
    }

    /// Open the tree containing the block map.
    pub fn block_tree(&self) -> Result<Arc<BlockTree>, MetaError> {
        Ok(self.shared.block_tree())
    }

    /// Check if a bucket with a given name exists.
    pub fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError> {
        super::buckets::bucket_exists(self, bucket_name)
    }

    /// Write an object record under `key`, releasing the blocks of whatever
    /// object it replaced (ADR 0008).
    ///
    /// The single entry point for every object-record write: PUT, the inline
    /// path, and `CompleteMultipartUpload`. An overwrite ends the replaced
    /// object's life exactly as a DELETE does, and pays the same cost -- one
    /// striped decrement per displaced occurrence. See
    /// [`write_path::create_object_meta`](super::write_path::create_object_meta)
    /// for the ordering rule (new record commits first) and why the reader
    /// race it inherits is the delete race and not a new one.
    pub async fn create_object_meta(
        &self,
        bucket_name: &str,
        key: impl AsRef<[u8]>,
        size: u64,
        hash: ContentHash,
        object_data: ObjectData,
    ) -> Result<Object, MetaError> {
        super::write_path::create_object_meta(
            self,
            bucket_name,
            key.as_ref(),
            size,
            hash,
            object_data,
        )
        .await
    }

    // get meta object from the DB
    pub fn get_object_meta(
        &self,
        bucket_name: &str,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<Object>, MetaError> {
        super::read_path::get_object_meta(self, bucket_name, key.as_ref())
    }

    pub fn get_object_paths(
        &self,
        bucket_name: &str,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<ObjectPaths>, MetaError> {
        super::read_path::get_object_paths(self, bucket_name, key.as_ref())
    }

    // create and insert a new  bucket
    pub fn create_bucket(&self, bucket_name: &str) -> Result<(), MetaError> {
        super::buckets::create_bucket(self, bucket_name)
    }

    /// Remove a bucket and its associated metadata.
    // TODO: this is very much not optimal
    pub async fn bucket_delete(&self, bucket_name: &str) -> Result<(), MetaError> {
        super::delete_path::bucket_delete(self, bucket_name).await
    }

    /// Record a new in-flight multipart upload (ADR 0003). Written by
    /// `CreateMultipartUpload` once the bucket check passes.
    pub fn create_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<(), MetaError> {
        super::uploads::create_upload(self, bucket, key, upload_id)
    }

    /// Read an upload record without claiming it: the non-atomic existence
    /// check `UploadPart` makes before it streams any block. See
    /// [`uploads::get_upload`](super::uploads::get_upload) for what this
    /// deliberately does not guarantee.
    pub fn get_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Option<UploadRecord>, MetaError> {
        super::uploads::get_upload(self, bucket, key, upload_id)
    }

    /// Claim an upload: the atomic read+remove that decides complete versus
    /// abort. `None` means another caller already won, and the caller answers
    /// `NoSuchUpload`.
    pub fn claim_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Option<UploadRecord>, MetaError> {
        super::uploads::claim_upload(self, bucket, key, upload_id)
    }

    /// Claim an upload together with the parts a `CompleteMultipartUpload`
    /// names: one transaction over both trees, so a named part record never
    /// outlives its upload record (ADR 0003 amendment).
    ///
    /// The returned parts are the values that transaction read, and the object
    /// must be built from them. See
    /// [`uploads::claim_upload_with_parts`](super::uploads::claim_upload_with_parts)
    /// for what each outcome means and what a crash after it leaves behind.
    pub fn claim_upload_with_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_numbers: &[i64],
    ) -> Result<UploadClaim, MetaError> {
        super::uploads::claim_upload_with_parts(self, bucket, key, upload_id, part_numbers)
    }

    /// Abort an upload: claim it, then remove each part record and drop the
    /// block references it held (ADR 0003).
    ///
    /// `Some(parts reaped)` when this caller won the claim, `None` when it
    /// lost -- the `NoSuchUpload` case, whether the upload was completed,
    /// aborted a moment earlier, or never existed. `AbortMultipartUpload` and
    /// the stale-upload GC are both callers; the GC is just another client of
    /// the claim.
    pub async fn abort_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Option<usize>, MetaError> {
        super::uploads::abort_upload(self, bucket, key, upload_id).await
    }

    /// Claim one part record by the storage key it is filed under, and drop
    /// the block references it held (ADR 0003).
    ///
    /// The take IS the claim: `Ok(None)` means another reaper -- a client's
    /// abort, a GC sweep -- won it and already released its blocks, which is
    /// a no-op for this caller and not an error. See
    /// [`uploads::reap_part`](super::uploads::reap_part) for why the key is
    /// passed in rather than rebuilt, and why the record leaves before its
    /// blocks do.
    ///
    /// Crate-internal: the daemon reaps through [`Self::abort_upload`] or the
    /// GC sweep, and the only caller that names a single raw key is fsck's
    /// `--repair`, which reaps the orphan parts its multipart pass found.
    pub(crate) async fn reap_part(
        &self,
        storage_key: &[u8],
    ) -> Result<Option<MultiPart>, MetaError> {
        super::uploads::reap_part(self, storage_key).await
    }

    /// Every in-flight upload record in the store, decoded, unsorted and
    /// unfiltered: `ListMultipartUploads` and the GC's TTL sweep each apply
    /// their own (in-memory) selection.
    pub fn list_uploads(&self) -> Result<Vec<UploadRecord>, MetaError> {
        super::uploads::list_uploads(self)
    }

    /// Every part record of one upload, ascending by part number.
    pub fn upload_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Vec<MultiPart>, MetaError> {
        super::uploads::upload_parts(self, bucket, key, upload_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_multipart_part(
        &self,
        bucket: String,
        key: String,
        size: usize,
        part_number: i64,
        upload_id: String,
        hash: ContentHash,
        blocks: Vec<BlockId>,
    ) -> Result<(), MetaError> {
        let mp_map = self.shared.multipart_tree();
        let storage_key = part_key(&bucket, &key, &upload_id, part_number);

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            upload_id = %upload_id,
            part_number = part_number,
            size = size,
            blocks = blocks.len(),
            "CasFS: insert_multipart_part"
        );

        let mp = MultiPart::new(size, part_number, bucket, key, upload_id, hash, blocks);

        mp_map.insert(&storage_key, mp)?;
        Ok(())
    }

    pub fn get_multipart_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
    ) -> Result<Option<MultiPart>, MetaError> {
        let mp_map = self.shared.multipart_tree();
        let storage_key = part_key(bucket, key, upload_id, part_number);

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            upload_id = %upload_id,
            part_number = part_number,
            "CasFS: get_multipart_part"
        );

        let result = mp_map.get_multipart_part(&storage_key);

        if let Ok(Some(ref mp)) = result {
            tracing::debug!(
                bucket = %bucket,
                key = %key,
                upload_id = %upload_id,
                part_number = part_number,
                blocks = mp.blocks().len(),
                "CasFS: get_multipart_part found"
            );
        }

        result
    }

    pub fn remove_multipart_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
    ) -> Result<(), MetaError> {
        let mp_map = self.shared.multipart_tree();
        let storage_key = part_key(bucket, key, upload_id, part_number);

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            upload_id = %upload_id,
            part_number = part_number,
            "CasFS: remove_multipart_part"
        );

        mp_map.remove(&storage_key)
    }

    pub fn key_exists(&self, bucket: &str, key: impl AsRef<[u8]>) -> Result<bool, MetaError> {
        super::buckets::key_exists(self, bucket, key.as_ref())
    }

    /// Get a list of all buckets in the system.
    pub fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError> {
        super::buckets::list_buckets(self)
    }

    /// Copy an object record into another bucket by REFERENCE: same blocks,
    /// one more reference each, no bytes moved (ADR 0014).
    ///
    /// `Ok(None)` means the clone did not happen and nothing was written --
    /// the source is gone, or a block it named was freed while its
    /// references were being taken. The caller stores the content the
    /// ordinary way instead.
    ///
    /// The source record's content must have been verified against its key
    /// (that is what a content-addressed namespace guarantees, and what
    /// makes the copy safe without re-hashing anything). See
    /// [`clone_path`](super::clone_path) for the ordering rules and the
    /// clone-versus-DELETE race.
    pub async fn clone_object_by_reference(
        &self,
        source_bucket: &str,
        source_key: impl AsRef<[u8]>,
        dest_bucket: &str,
        dest_key: impl AsRef<[u8]>,
    ) -> Result<Option<Object>, MetaError> {
        super::clone_path::clone_object_by_reference(
            self,
            source_bucket,
            source_key.as_ref(),
            dest_bucket,
            dest_key.as_ref(),
        )
        .await
    }

    /// Delete an object from a bucket, answering whether a record was there
    /// to delete. It also deletes the keys under its tree.
    ///
    /// Idempotent: an absent key is `Ok(false)`, not an error. The answer
    /// comes out of the same transaction that took the record, so it is the
    /// count a Redis-style DEL reply needs (respcas, ADR 0014) rather than a
    /// separate lookup that could race with another deleter.
    pub async fn delete_object(
        &self,
        bucket: &str,
        key: impl AsRef<[u8]>,
    ) -> Result<bool, MetaError> {
        super::delete_path::delete_object(self, bucket, key.as_ref()).await
    }

    // convenient function to store an object to disk and then store it's metada
    pub async fn store_single_object_and_meta(
        &self,
        bucket_name: &str,
        key: impl AsRef<[u8]>,
        data: AsyncByteStream,
        len: usize,
    ) -> io::Result<Object> {
        super::write_path::store_single_object_and_meta(self, bucket_name, key.as_ref(), data, len)
            .await
    }

    /// Save the stream of bytes to disk.
    ///
    /// The data is streamed in chunks, and each chunk is hashed and stored on disk.
    /// The hash of each chunk is used as a key to store the data in the database.
    ///
    /// A list of block ID's used as keys for the data blocks is
    /// returned, along with the hash of the full byte stream, and the length of the stream.
    pub async fn store_object(
        &self,
        bucket_name: &str,
        key: impl AsRef<[u8]>,
        data: AsyncByteStream,
    ) -> io::Result<(Vec<BlockId>, ContentHash, u64)> {
        super::write_path::store_object(self, bucket_name, key.as_ref(), data).await
    }

    /// Store an object inlined in its own metadata record.
    ///
    /// Inline objects hold no block references, but the write still releases
    /// what it replaced: an inline write over a BLOCK-BACKED object is the
    /// case where the new record names nothing, so the release is the only
    /// thing between the overwrite and a permanent leak (ADR 0008).
    pub async fn store_inlined_object(
        &self,
        bucket_name: &str,
        key: impl AsRef<[u8]>,
        data: Vec<u8>,
    ) -> Result<Object, MetaError> {
        super::write_path::store_inlined_object(self, bucket_name, key.as_ref(), data).await
    }
}

#[cfg(test)]
mod tests;
