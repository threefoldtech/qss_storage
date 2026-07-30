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

#[derive(Parser, Debug)]
pub struct RetrieveConfig {
    #[arg(long, default_value = ".")]
    pub meta_root: PathBuf,

    #[arg(long, default_value = ".")]
    pub fs_root: PathBuf,

    #[arg(
        long,
        default_value = "fjall",
        help = "Metadata DB  (fjall, fjall_notx)"
    )]
    pub metadata_db: StorageEngine,

    #[arg(required = true, help = "Bucket name")]
    pub bucket: String,

    #[arg(required = true, help = "Object key")]
    pub key: String,

    #[arg(required = true, help = "Destination file path")]
    pub dest: String,
}

#[tokio::main]
pub async fn retrieve(args: RetrieveConfig) -> Result<()> {
    let storage_engine = args.metadata_db;
    let metrics = SharedMetrics::new();
    let casfs = CasFS::single_namespace(
        args.fs_root.clone(),
        args.meta_root.clone(),
        metrics.to_cas(),
        storage_engine,
        None,
        None,
        None,
        false, // verify_on_read
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

    debug_assert!(obj_meta.size() as usize == block_size);
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
