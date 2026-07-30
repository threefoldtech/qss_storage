//! `s3cas check`: re-verify one stored object against both hashes it carries.
//!
//! Two independent checks, because the ADR 0002 split gives the two hashes
//! different jobs:
//!
//! - every block file is re-hashed with the *store's* hasher (BLAKE3 at the
//!   width in the store header) and compared to the address it is filed
//!   under. This is what catches bitrot, and it names the block that rotted.
//! - the assembled object is re-hashed with MD5 and compared to the stored
//!   `ContentHash`, which is the S3 ETag. This catches a wrong block list --
//!   right blocks, wrong order or wrong set -- which per-block checks cannot
//!   see.

use std::fmt::{self, Display, Formatter};
use std::path::PathBuf;

use anyhow::{Result, bail};
use bytes::Bytes;
use clap::Parser;
use futures::StreamExt;
use md5::{Digest, Md5};

use crate::metrics::SharedMetrics;
use crate::store_options::StoreOptions;
use cas_storage::BlockStream;
use cas_storage::CasFS;
use cas_storage::ContentHash;
use cas_storage::Hasher;
use cas_storage::RangeRequest;
use cas_storage::StorageEngine;
use cas_storage::metastore::{BlockId, Object};

#[derive(Parser, Debug)]
pub struct CheckConfig {
    #[arg(
        long,
        help = "Path to qss_storage.toml (default: ./qss_storage.toml, then \
                /etc/qss_storage/qss_storage.toml)"
    )]
    pub config: Option<PathBuf>,

    #[arg(long, default_value = ".")]
    pub meta_root: PathBuf,

    #[arg(long, default_value = ".")]
    pub fs_root: PathBuf,

    #[arg(long, help = "Metadata DB  (fjall, fjall_notx); default fjall")]
    pub metadata_db: Option<StorageEngine>,

    #[arg(required = true, help = "Bucket name")]
    pub bucket: String,

    #[arg(required = true, help = "Object key")]
    pub key: String,
}

/// What went wrong with one block of the object under check.
#[derive(Debug)]
pub enum BlockFault {
    /// The block file could not be read at all.
    Unreadable {
        id: BlockId,
        path: PathBuf,
        error: String,
    },
    /// The block file's bytes hash to something other than its address.
    Mismatch {
        id: BlockId,
        path: PathBuf,
        actual: BlockId,
    },
}

impl Display for BlockFault {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            BlockFault::Unreadable { id, path, error } => write!(
                f,
                "block {}: cannot read {}: {error}",
                id.to_hex(),
                path.display()
            ),
            BlockFault::Mismatch { id, path, actual } => write!(
                f,
                "block {}: file {} hashes to {}",
                id.to_hex(),
                path.display(),
                actual.to_hex()
            ),
        }
    }
}

/// Re-hashes every block file of `obj_meta` with `hasher` and reports the ones
/// that do not hash to the address they are filed under.
///
/// `paths` comes from `CasFS::get_object_paths`, which builds it from
/// `obj_meta.blocks()` in order, so the two zip index for index. An inlined
/// object has neither blocks nor paths and trivially passes: its bytes live in
/// the metadata record, which the ETag check covers.
///
/// Whole files are read into memory. A block is at most `BLOCK_SIZE` (1 MiB),
/// and this is a one-object operator command, not a serving path.
fn verify_blocks(hasher: Hasher, obj_meta: &Object, paths: &[(PathBuf, usize)]) -> Vec<BlockFault> {
    let mut faults = Vec::new();

    for (id, (path, _size)) in obj_meta.blocks().iter().zip(paths) {
        match std::fs::read(path) {
            Err(e) => faults.push(BlockFault::Unreadable {
                id: *id,
                path: path.clone(),
                error: e.to_string(),
            }),
            Ok(bytes) => {
                let actual = hasher.hash(&bytes);
                if actual != *id {
                    faults.push(BlockFault::Mismatch {
                        id: *id,
                        path: path.clone(),
                        actual,
                    });
                }
            }
        }
    }

    faults
}

/// `store` is the merge of this command's flags with the config file; see
/// [`StoreOptions`].
#[tokio::main]
pub async fn check_integrity(args: CheckConfig, store: StoreOptions) -> Result<()> {
    let metrics = SharedMetrics::new();
    let casfs = CasFS::single_namespace(
        args.fs_root.clone(),
        args.meta_root.clone(),
        metrics.to_cas(),
        store.metadata_db,
        store.inline_metadata_size,
        Some(store.durability),
        Some(store.header_spec()),
        // verify_on_read stays off whatever the config says: this command
        // checks the blocks itself and reports every one that failed, which a
        // read-path corruption error would pre-empt at the first bad block.
        false,
    )?;

    let Some((obj_meta, paths)) = casfs.get_object_paths(&args.bucket, &args.key)? else {
        eprintln!("Object not found");
        return Ok(());
    };

    // Block-level first: it says *which* block is wrong, where the ETag can
    // only say the object is.
    let faults = verify_blocks(casfs.hasher(), &obj_meta, &paths);
    for fault in &faults {
        eprintln!("check failed: {fault}");
    }

    let data = read_object(&obj_meta, paths, metrics).await?;
    let hash = ContentHash(Md5::digest(data).into());
    let etag_ok = hash == *obj_meta.hash();
    if !etag_ok {
        eprintln!(
            "check failed: object hash mismatch: metadata says {}, content is {}",
            obj_meta.hash().to_hex(),
            hash.to_hex()
        );
    }

    if !faults.is_empty() || !etag_ok {
        bail!(
            "{} of {} blocks failed verification; object hash {}",
            faults.len(),
            obj_meta.blocks().len(),
            if etag_ok { "matched" } else { "mismatched" }
        );
    }

    println!(
        "check passed: {} blocks verified with {}/{}, object hash matched",
        obj_meta.blocks().len(),
        casfs.hasher().algo_name(),
        casfs.hasher().width()
    );
    Ok(())
}

/// Reads the object's bytes, from the metadata record if it is inlined and
/// from its block files otherwise.
async fn read_object(
    obj_meta: &Object,
    paths: Vec<(PathBuf, usize)>,
    metrics: SharedMetrics,
) -> Result<Vec<u8>> {
    if let Some(inline_data) = obj_meta.inlined() {
        return Ok(inline_data.to_vec());
    }

    let block_size: usize = paths.iter().map(|(_, size)| size).sum();
    debug_assert!(obj_meta.size() == block_size as u64);

    let mut block_stream = BlockStream::new(paths, block_size, RangeRequest::All, metrics.to_cas());
    let mut data = Vec::with_capacity(block_size);

    while let Some(chunk_result) = block_stream.next().await {
        let chunk: Bytes = chunk_result?;
        data.extend_from_slice(&chunk);
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cas_storage::{AsyncByteStream, Durability, SharedMetrics};
    use futures::stream;
    use std::path::Path;
    use tempfile::TempDir;

    /// Big enough to be split into more than one block, so the report has to
    /// pick the right one out of several.
    const OBJECT_SIZE: usize = 3 * cas_storage::cas::fs::BLOCK_SIZE + 17;

    fn options() -> StoreOptions {
        StoreOptions {
            metadata_db: StorageEngine::Fjall,
            durability: Durability::Buffer,
            inline_metadata_size: Some(1),
            verify_on_read: false,
            hasher: Hasher::Blake3W32,
        }
    }

    fn casfs_at(dir: &TempDir, opts: &StoreOptions) -> CasFS {
        CasFS::single_namespace(
            dir.path().to_path_buf(),
            dir.path().to_path_buf(),
            SharedMetrics::default(),
            opts.metadata_db,
            opts.inline_metadata_size,
            Some(opts.durability),
            Some(opts.header_spec()),
            false,
        )
        .unwrap()
    }

    /// Stores one multi-block object and hands back the store it lives in.
    #[allow(clippy::cast_possible_truncation)] // the modulus bounds the cast
    async fn store_object(dir: &TempDir, opts: &StoreOptions) -> CasFS {
        let casfs = casfs_at(dir, opts);
        casfs.create_bucket("bucket").unwrap();
        let data: Vec<u8> = (0..OBJECT_SIZE).map(|i| (i % 251) as u8).collect();
        let len = data.len();
        let stream = AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(data)) }));
        casfs
            .store_single_object_and_meta("bucket", "key", stream, len)
            .await
            .unwrap();
        casfs
    }

    /// Flips one bit in a block file, the way bitrot would.
    fn flip_a_byte(path: &Path) {
        let mut bytes = std::fs::read(path).unwrap();
        bytes[0] ^= 0xff;
        std::fs::write(path, bytes).unwrap();
    }

    #[tokio::test]
    async fn a_healthy_object_has_no_faults() {
        let dir = TempDir::new().unwrap();
        let opts = options();
        let casfs = store_object(&dir, &opts).await;
        let (obj_meta, paths) = casfs.get_object_paths("bucket", "key").unwrap().unwrap();

        assert!(paths.len() > 1, "the fixture must span several blocks");
        assert!(verify_blocks(casfs.hasher(), &obj_meta, &paths).is_empty());
    }

    #[tokio::test]
    async fn a_flipped_byte_names_the_block_and_its_path() {
        let dir = TempDir::new().unwrap();
        let opts = options();
        let casfs = store_object(&dir, &opts).await;
        let (obj_meta, paths) = casfs.get_object_paths("bucket", "key").unwrap().unwrap();

        let (rotted_path, _) = paths[1].clone();
        flip_a_byte(&rotted_path);

        let faults = verify_blocks(casfs.hasher(), &obj_meta, &paths);
        assert_eq!(faults.len(), 1, "{faults:?}");
        let BlockFault::Mismatch { id, path, actual } = &faults[0] else {
            panic!("expected a hash mismatch, got {:?}", faults[0]);
        };
        assert_eq!(id, &obj_meta.blocks()[1]);
        assert_eq!(path, &rotted_path);
        assert_ne!(actual, id);

        let text = faults[0].to_string();
        assert!(text.contains(&id.to_hex()), "{text}");
        assert!(text.contains(&rotted_path.display().to_string()), "{text}");
    }

    #[tokio::test]
    async fn a_missing_block_file_is_reported_not_panicked_on() {
        let dir = TempDir::new().unwrap();
        let opts = options();
        let casfs = store_object(&dir, &opts).await;
        let (obj_meta, paths) = casfs.get_object_paths("bucket", "key").unwrap().unwrap();

        std::fs::remove_file(&paths[0].0).unwrap();

        let faults = verify_blocks(casfs.hasher(), &obj_meta, &paths);
        assert_eq!(faults.len(), 1, "{faults:?}");
        assert!(matches!(faults[0], BlockFault::Unreadable { .. }));
    }

    /// Verification uses the hash in the store's header, not a fixed one, so
    /// a width-16 store checks out with width-16 addresses.
    #[tokio::test]
    async fn verification_follows_the_stores_own_width() {
        let dir = TempDir::new().unwrap();
        let mut opts = options();
        opts.hasher = Hasher::Blake3W16;
        let casfs = store_object(&dir, &opts).await;
        assert_eq!(casfs.hasher(), Hasher::Blake3W16);

        let (obj_meta, paths) = casfs.get_object_paths("bucket", "key").unwrap().unwrap();
        assert_eq!(obj_meta.blocks()[0].len(), 16);
        assert!(verify_blocks(casfs.hasher(), &obj_meta, &paths).is_empty());

        // The wrong hasher must not silently pass: 32 byte addresses never
        // equal 16 byte ones.
        let faults = verify_blocks(Hasher::Blake3W32, &obj_meta, &paths);
        assert_eq!(faults.len(), paths.len());
    }
}
