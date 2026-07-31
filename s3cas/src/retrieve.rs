use std::path::PathBuf;

use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;

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
