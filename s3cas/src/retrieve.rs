use std::path::PathBuf;

use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;

use crate::inspect::refuse_unless_store_exists;
use crate::metrics::SharedMetrics;
use cas_storage::BlockStream;
use cas_storage::CasFS;
use cas_storage::RangeRequest;
use cas_storage::StorageEngine;
use cas_storage::StoreOptions;

#[derive(Parser, Debug)]
pub struct RetrieveConfig {
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

    #[arg(long, help = "Metadata DB (fjall); default fjall")]
    pub metadata_db: Option<StorageEngine>,

    #[arg(required = true, help = "Bucket name")]
    pub bucket: String,

    #[arg(required = true, help = "Object key")]
    pub key: String,

    #[arg(required = true, help = "Destination file path")]
    pub dest: String,
}

/// `store` is the merge of this command's flags with the config file; see
/// [`StoreOptions`].
#[tokio::main]
pub async fn retrieve(args: RetrieveConfig, store: StoreOptions) -> Result<()> {
    // Before the constructor, which is what creates: on a mistyped --meta-root
    // this command used to build an empty store and report "Object not found".
    refuse_unless_store_exists(&args.meta_root)?;

    let metrics = SharedMetrics::new();
    let casfs = CasFS::single_namespace(
        args.fs_root.clone(),
        args.meta_root.clone(),
        metrics.to_cas(),
        store.metadata_db,
        store.inline_metadata_size,
        Some(store.durability),
        Some(store.header_spec()),
        store.verify_on_read,
    )?;

    let (obj_meta, paths) = match casfs.get_object_paths(&args.bucket, &args.key)? {
        Some((obj, paths)) => (obj, paths),
        None => {
            eprintln!("Object not found");
            return Ok(());
        }
    };

    if let Some(data) = obj_meta.inlined() {
        let mut file = tokio::fs::File::create(&args.dest).await?;
        file.write_all(data).await?;
        return Ok(());
    }

    let block_size: usize = paths.iter().map(|(_, size)| size).sum();

    debug_assert!(obj_meta.size() == block_size as u64);
    let mut block_stream = BlockStream::new(paths, block_size, RangeRequest::All, metrics.to_cas());

    // Create the destination file
    let mut file = tokio::fs::File::create(&args.dest).await?;

    // Read from block stream and write to file
    while let Some(chunk_result) = block_stream.next().await {
        let chunk: Bytes = chunk_result?;
        file.write_all(&chunk).await?;
    }

    // Ensure all data is written to disk
    file.flush().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cas_storage::{Durability, Hasher};
    use tempfile::TempDir;

    fn options() -> StoreOptions {
        StoreOptions {
            metadata_db: StorageEngine::Fjall,
            durability: Durability::Buffer,
            inline_metadata_size: Some(1),
            verify_on_read: false,
            hasher: Hasher::Blake3W32,
        }
    }

    /// A mistyped `--meta-root` is a refusal, not a store creation.
    ///
    /// Not a `#[tokio::test]`: `retrieve` carries `#[tokio::main]` and builds
    /// its own runtime, which panics if one is already running.
    #[test]
    fn a_missing_store_is_refused_and_nothing_is_created() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("typo");
        let dest = dir.path().join("out.bin");

        let args = RetrieveConfig {
            config: None,
            meta_root: missing.clone(),
            fs_root: missing.clone(),
            metadata_db: None,
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            dest: dest.display().to_string(),
        };

        let err = retrieve(args, options()).expect_err("a missing store must be refused");
        let msg = err.to_string();
        assert!(msg.contains("no store at"), "{msg}");
        assert!(
            msg.contains(&missing.display().to_string()),
            "the message must name the path: {msg}"
        );

        // The point of the guard: the refusal happens before anything is
        // constructed, so the mistyped path is still not a store.
        assert!(
            !missing.exists(),
            "the guard must refuse before the constructor creates {}",
            missing.display()
        );
        // And no half-written destination either: the refusal precedes the
        // File::create as well.
        assert!(!dest.exists(), "nothing must have been written to {dest:?}");
    }
}
