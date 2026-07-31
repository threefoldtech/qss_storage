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

// Re-export the block hasher (used by both the cas and metastore layers)
pub use hasher::{Hasher, HasherError};

// Re-export the config file types (the binaries merge these with their flags)
pub use config::{
    ConfigError, HashConfig, MetricsConfig, QssStorageConfig, RespConfig, S3Config, StoreConfig,
};

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
    CasFS,
    SharedBlockStore,
    StorageEngine,
    // Streaming and utilities
    block_stream::{BlockCorruption, BlockStream},
    // Multipart support
    multipart::{MultiPart, MultiPartTree},
    range_request::{RangeRequest, parse_range_request},
};

// Re-export metrics types
pub use metrics::{MetricsCollector, NoOpMetrics, SharedMetrics};
