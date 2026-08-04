//! The fjall metadata backend: fjall's single-writer transactional database.
//!
//! This was once two backends (transactional and not) sharing a generic
//! flavor layer in `fjall_common.rs`; ADR 0007 removed the non-transactional
//! one and the generic layer was folded back in here. Writes go through a
//! real fjall write transaction, so a rollback discards them without touching
//! the keyspace; the price is that only one writer may be in flight at a
//! time.
//!
//! # Where the durability level is applied
//!
//! In exactly one place, [`TxDb::persist`], and on every path that ends in an
//! acknowledgement: after a transaction commits, and after a bare tree
//! `insert` or `remove`. The second half was missing until the ADR 0011
//! rider, and the gap was invisible to a kill-based test: fjall persists its
//! own journal writes with `PersistMode::Buffer` regardless of what the store
//! was configured with, which reaches the kernel (so `kill -9` cannot take
//! it) but never fsyncs (so a power cut can). Every ack-carrying write that
//! did not go through a transaction -- `CreateBucket`,
//! `CreateMultipartUpload`, `UploadPart`, respcas's `SET` and `DEL` -- was
//! therefore page-cache-only even at `fsync` durability.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use fjall::{self, KeyspaceCreateOptions, Readable, SingleWriterTxKeyspace};

use crate::metastore::{
    BaseMetaTree, Durability, MULTIPART_PARTS_TREE, MetaError, MetaTreeExt, Store, Transaction,
    TransactionBackend, UPLOADS_TREE,
};

mod tree;
use tree::FjallTree;
#[cfg(test)]
mod tests;

/// Objects at or below this size have their data inlined in the metadata
/// record instead of being written to the block store. Set very low so that
/// inlining is practically disabled unless a caller asks for it.
pub const DEFAULT_INLINED_METADATA_SIZE: usize = 1;

/// Database handle: the fjall database plus the persist mode applied after
/// every commit.
#[derive(Clone)]
pub struct TxDb {
    db: Arc<fjall::SingleWriterTxDatabase>,
    durability: fjall::PersistMode,
}

impl TxDb {
    fn commit_persist(&self, tx: fjall::SingleWriterWriteTx) -> Result<(), MetaError> {
        tx.commit()
            .map_err(|e| MetaError::TransactionError(e.to_string()))?;

        self.persist()
    }

    /// Persists the journal at this store's configured durability.
    ///
    /// The one place a write becomes as durable as the ack about to be sent
    /// claims. What each mode does, in fjall 3.1.8's journal writer
    /// (`journal/writer.rs`), is worth stating because the distinction is the
    /// whole contract:
    ///
    /// - both modes FIRST flush the journal's userspace `BufWriter` (8 KiB)
    ///   with `write`, so the bytes are the kernel's and a process death
    ///   cannot take them;
    /// - `Buffer` stops there -- no fsync, the page cache decides when the
    ///   platter sees it;
    /// - `SyncAll` then fsyncs, so a power cut cannot take it either.
    ///
    /// That is exactly the two-level contract `Durability` promises, which is
    /// why this is called on every path that acknowledges a write and not
    /// only after transactions.
    fn persist(&self) -> Result<(), MetaError> {
        self.db
            .persist(self.durability)
            .map_err(|e| MetaError::PersistError(e.to_string()))
    }

    /// The persist mode this handle applies. Test-facing: it is how the
    /// durability plumbing is pinned, since an fsync leaves nothing an
    /// in-process assertion can look at.
    #[cfg(test)]
    pub(crate) fn durability(&self) -> fjall::PersistMode {
        self.durability
    }
}

/// Metadata store backed by fjall's single-writer transactional database.
#[derive(Clone)]
pub struct FjallStore {
    db: TxDb,
    inlined_metadata_size: usize,
    /// Keyspace handles are cached by name so that a hot path does not take
    /// the database's keyspace lock on every tree access. Entries are evicted
    /// by [`Store::tree_delete`].
    partition_cache: Arc<Mutex<HashMap<String, Arc<SingleWriterTxKeyspace>>>>,
}

impl std::fmt::Debug for FjallStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallStore")
            .field("db", &"<fjall::SingleWriterTxDatabase>")
            .finish()
    }
}

impl FjallStore {
    /// Opens (creating if absent) the fjall database at `path`.
    ///
    /// # Errors
    ///
    /// [`MetaError::StoreLocked`] if another process already holds the
    /// database's lock -- fjall takes an exclusive lock on the directory, so
    /// this is the routine answer whenever the daemon is running and an
    /// offline tool is pointed at its store. Any other open failure becomes
    /// [`MetaError::OtherDBError`].
    ///
    /// This used to `.unwrap()`. Lock contention is an expected condition, not
    /// a bug, and the tools' exit-code contracts (fsck's exit 3) depend on it
    /// arriving as a value.
    pub fn new(
        path: PathBuf,
        inlined_metadata_size: Option<usize>,
        durability: Option<Durability>,
    ) -> Result<Self, MetaError> {
        tracing::debug!("Opening fjall store at {:?}", path);

        let db = fjall::SingleWriterTxDatabase::builder(&path)
            .open()
            .map_err(|e| match e {
                fjall::Error::Locked => MetaError::StoreLocked(path.display().to_string()),
                other => MetaError::OtherDBError(format!(
                    "cannot open metadata store at {}: {other}",
                    path.display()
                )),
            })?;

        // Two levels since ADR 0010. `fsync` persists the journal with data
        // AND metadata (fjall SyncAll, the strongest mode); `buffer` leaves
        // the flush to the page cache. fjall's SyncData has no user-facing
        // level any more: on an append-only journal it must flush the size
        // metadata anyway, so it bought nothing that SyncAll does not.
        let durability = match durability.unwrap_or(Durability::Fsync) {
            Durability::Buffer => fjall::PersistMode::Buffer,
            Durability::Fsync => fjall::PersistMode::SyncAll,
        };

        // The one line that tells an operator reading "fsync" in the config
        // that the contract has a table (ADR 0013).
        if durability == fjall::PersistMode::SyncAll {
            tracing::info!(
                "ack durability contract (ADR 0013): {MULTIPART_PARTS_TREE} and {UPLOADS_TREE} \
                 are recoverable-class (kernel-visible, no per-ack fsync; loss is loud and \
                 retryable); every other tree persists each ack at the configured durability"
            );
        }

        Ok(Self {
            db: TxDb {
                db: Arc::new(db),
                durability,
            },
            inlined_metadata_size: inlined_metadata_size.unwrap_or(DEFAULT_INLINED_METADATA_SIZE),
            partition_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn db(&self) -> &TxDb {
        &self.db
    }

    pub fn get_inlined_metadata_size(&self) -> usize {
        self.inlined_metadata_size
    }

    /// Returns the cached handle for `name`, opening (and creating) the
    /// keyspace on first use.
    pub fn get_partition(&self, name: &str) -> Result<Arc<SingleWriterTxKeyspace>, MetaError> {
        let mut cache = self
            .partition_cache
            .lock()
            .expect("Can lock partition cache");
        if let Some(partition) = cache.get(name) {
            return Ok(partition.clone());
        }
        let partition = Arc::new(
            self.db
                .db
                .keyspace(name, KeyspaceCreateOptions::default)
                .map_err(|e| MetaError::OtherDBError(e.to_string()))?,
        );
        cache.insert(name.to_string(), partition.clone());
        Ok(partition)
    }

    /// Exact key count of one keyspace, via a read transaction.
    fn count_keys(&self, partition: &SingleWriterTxKeyspace) -> Result<usize, MetaError> {
        self.db
            .db
            .read_tx()
            .len(partition)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))
    }
}

/// Which persist follows a bare acknowledged write on a tree (ADR 0013).
///
/// Both classes are kernel-visible before the ack returns (fjall persists
/// bare writes at its internal `PersistMode::Buffer`, pinned by the
/// journal-bytes tests below), so `kill -9` takes neither. They differ only
/// against power loss, and only where the protocol would not notice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AckPersist {
    /// Loss after the ack would be silent or terminal (object records,
    /// bucket metadata, respcas's SET/DEL): persist at the store's
    /// configured durability before returning.
    Contract,
    /// Loss after the ack is caught loudly by a mandatory later step of
    /// the same protocol -- a vanished part record fails the complete
    /// with InvalidPart, a vanished upload marker fails the next
    /// upload-part with NoSuchUpload -- and the client recovers by
    /// retrying. fjall's internal kernel-visible write is enough; the
    /// per-ack fsync this skips was 55-71% of parallel ingest (the
    /// 2026-08-02 campaign re-baseline).
    Recoverable,
}

/// The ADR 0013 contract table, keyed by tree name.
pub(crate) fn ack_persist_for(name: &str) -> AckPersist {
    match name {
        MULTIPART_PARTS_TREE | UPLOADS_TREE => AckPersist::Recoverable,
        _ => AckPersist::Contract,
    }
}

impl Store for FjallStore {
    fn tree_open(&self, name: &str) -> Result<Arc<dyn BaseMetaTree>, MetaError> {
        Ok(Arc::new(FjallTree::new(
            self.db.clone(),
            self.get_partition(name)?,
            ack_persist_for(name),
        )))
    }

    fn tree_ext_open(&self, name: &str) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError> {
        Ok(Arc::new(FjallTree::new(
            self.db.clone(),
            self.get_partition(name)?,
            ack_persist_for(name),
        )))
    }

    fn tree_exists(&self, name: &str) -> Result<bool, MetaError> {
        Ok(self.db.db.keyspace_exists(name))
    }

    // The database's own keyspace registry is the authority, not the
    // partition cache: a tree this process never opened is still a tree.
    fn list_trees(&self) -> Result<Vec<String>, MetaError> {
        Ok(self
            .db
            .db
            .list_keyspace_names()
            .iter()
            .map(ToString::to_string)
            .collect())
    }

    fn tree_delete(&self, name: &str) -> Result<(), MetaError> {
        let partition = self.get_partition(name)?;
        // Drop the cached handle first: after the delete it refers to a
        // keyspace that no longer exists.
        self.partition_cache
            .lock()
            .expect("Can lock partition cache")
            .remove(name);
        // `delete_keyspace` lives on the plain database underneath and takes a
        // plain keyspace handle by value.
        self.db
            .db
            .inner()
            .delete_keyspace(partition.inner().clone())
            .map_err(|e| MetaError::OtherDBError(e.to_string()))
    }

    fn begin_transaction(&self) -> Transaction {
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
        //    `Arc<FjallStore>` cloned from `self`, and the store's database
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
                self.db.db.write_tx(),
            )
        };

        Transaction::new(Box::new(FjallTransaction::new(tx, Arc::new(self.clone()))))
    }

    // Upstream leaves `Store::num_keys` `unimplemented!()` on this backend,
    // which panics on the default `--metadata-db fjall` path. A read
    // transaction counts a keyspace fine -- it is the same call `len` uses.
    fn num_keys(&self, tree_name: &str) -> Result<usize, MetaError> {
        let partition = self.get_partition(tree_name)?;
        self.count_keys(&partition)
    }

    fn disk_space(&self) -> u64 {
        self.db.db.disk_space().unwrap_or(0)
    }
}

pub struct FjallTransaction {
    // FIELD ORDER IS LOAD-BEARING. DO NOT REORDER.
    //
    // `tx` holds a transaction whose lifetime was laundered to `'static` in
    // `Store::begin_transaction`; it borrows the database owned (via
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

    fn remove(&mut self, tree_name: &str, key: &[u8]) -> Result<(), MetaError> {
        let partition = self.store.get_partition(tree_name)?;
        if let Some(ref mut tx) = self.tx {
            tx.remove(&partition, key);
            Ok(())
        } else {
            Err(MetaError::TransactionError(
                "Transaction already rolled back".to_string(),
            ))
        }
    }
}
