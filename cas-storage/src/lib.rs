//! # CAS Storage Library
//!
//! A content-addressable storage library with block-level deduplication,
//! reference counting, and multi-user support.
//!
//! ## Features
//!
//! - **Content-Addressable Storage**: Objects chunked into 1 MiB blocks, addressed by BLAKE3 hash
//!   (the object ETag stays MD5, as S3 requires)
//! - **Block Deduplication**: Duplicate blocks stored only once with reference counting
//! - **Multi-User Support**: Shared block storage with isolated metadata per user
//! - **Transactional Metadata**: Fjall-backed metadata store with real transactions
//! - **Inline Data**: Small objects can be stored directly in metadata
//! - **Streaming I/O**: Efficient streaming reads and writes
//!
//! ## Example: Single-namespace convenience
//!
//! ```no_run
//! use cas_storage::{CasFS, StorageEngine, Durability};
//! use std::path::PathBuf;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let casfs = CasFS::single_namespace(
//!     PathBuf::from("./data"),
//!     PathBuf::from("./data/meta"),
//!     Default::default(),  // metrics
//!     StorageEngine::Fjall,
//!     None,                // inlined_metadata_size
//!     Some(Durability::Fsync),
//!     None,                // header spec (defaults to blake3/32)
//!     false,               // verify_on_read
//!     None,                // stripe count (defaults to 1024)
//!     None,                // blocks per commit (defaults to 64)
//!     None,                // group commit (ADR 0011; defaults to off)
//! )?;
//! casfs.create_bucket("my-bucket")?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Example: Multi-namespace (shared block store, many namespaces)
//!
//! ```no_run
//! use cas_storage::{SharedBlockStore, CasFS, StorageEngine, Durability};
//! use std::path::PathBuf;
//! use std::sync::Arc;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create shared block store (once, shared across all namespaces). It
//! // owns the blocks DB AND the block data root: every namespace derives
//! // block file paths from that one root.
//! let shared = Arc::new(SharedBlockStore::new(
//!     PathBuf::from("./data/meta/blocks"),
//!     PathBuf::from("./data/blocks"),
//!     StorageEngine::Fjall,
//!     None,
//!     Some(Durability::Fsync),
//!     None,                // header spec (defaults to blake3/32)
//!     None,                // stripe count (defaults to 1024)
//!     None,                // blocks per commit (defaults to 64)
//!     None,                // group commit (ADR 0011; defaults to off)
//! )?);
//!
//! // One CasFS per namespace (e.g. per user)
//! let alice = CasFS::new(
//!     PathBuf::from("./data/meta/user_alice"),
//!     shared.clone(),
//!     Default::default(),
//!     StorageEngine::Fjall,
//!     None,
//!     Some(Durability::Fsync),
//!     false,               // verify_on_read
//! )?;
//! # Ok(())
//! # }
//! ```

pub mod cas;
pub mod config;
pub mod hasher;
pub mod metastore;
pub mod metrics;
pub mod scrub;
pub mod store_options;

// Re-export the block hasher (used by both the cas and metastore layers)
pub use hasher::{Hasher, HasherError};

// Re-export the config file types (the binaries merge these with their flags)
pub use config::{
    ConfigError, HashConfig, MetricsConfig, MultipartConfig, QssStorageConfig, RespConfig,
    S3Config, StoreConfig,
};

// Re-export the resolved store settings every binary opens its store with
pub use store_options::StoreOptions;

// Re-export main types from metastore
pub use metastore::{
    // Storage abstractions
    BaseMetaTree,
    // Metadata structures
    Block,
    BlockId,
    BlockTree,
    BucketMeta,
    ContentHash,
    // Storage backends
    Durability,
    FjallStore,
    // Store header (format versioning)
    HeaderSpec,
    MetaError,
    MetaStore,
    MetaTreeExt,
    Object,
    ObjectData,
    ObjectType,
    Store,
    StoreHeader,
    StoreHeaderError,
    Transaction,
};

// Re-export main types from cas
pub use cas::{
    // Core storage
    AsyncByteStream,
    // Streaming and utilities
    BLOCKS_DB_DIR_NAME,
    CasFS,
    // Cross-request group commit (ADR 0011): how a store runs its commit
    // station, and what that station has done
    GroupCommit,
    GroupCommitStats,
    SharedBlockStore,
    StorageEngine,
    // Stale-upload GC (ADR 0003): what one sweep did, and the sweep itself
    // (the s3cas daemon task's only entry point into it)
    SweepStats,
    // The outcome of complete's atomic upload-plus-parts claim
    UploadClaim,
    block_stream::{BlockCorruption, BlockStream},
    // Multipart support
    multipart::{MultiPart, MultiPartTree},
    range_request::RangeRequest,
    sweep_stale_uploads,
};

// Re-export metrics types
pub use metrics::{MetricsCollector, NoOpMetrics, SharedMetrics};
