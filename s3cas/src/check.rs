use std::path::PathBuf;

use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use futures::StreamExt;
use md5::{Digest, Md5};

use crate::metrics::SharedMetrics;
use crate::store_options::StoreOptions;
use cas_storage::BlockStream;
use cas_storage::CasFS;
use cas_storage::ContentHash;
use cas_storage::RangeRequest;
use cas_storage::StorageEngine;

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
        // checks the object hash itself and reports the mismatch, which a
        // read-path corruption error would pre-empt with a different message.
        false,
    )?;

    let (obj_meta, _) = match casfs.get_object_paths(&args.bucket, &args.key)? {
        Some((obj, paths)) => (obj, paths),
        None => {
            eprintln!("Object not found");
            return Ok(());
        }
    };

    let Some(data) = get_object_data(&casfs, &args.bucket, &args.key, metrics).await? else {
        eprintln!("Object not found");
        return Ok(());
    };

    let hash = ContentHash(Md5::digest(data).into());
    if hash != *obj_meta.hash() {
        eprintln!("check failed: hash mismatch");
    } else {
        println!("check passed: hash matched");
    }

    Ok(())
}

async fn get_object_data(
    casfs: &CasFS,
    bucket: &str,
    key: &str,
    metrics: SharedMetrics,
) -> Result<Option<Vec<u8>>> {
    let (obj_meta, paths) = match casfs.get_object_paths(bucket, key)? {
        Some((obj, paths)) => (obj, paths),
        None => return Ok(None),
    };

    let data = if let Some(inline_data) = obj_meta.inlined() {
        inline_data.to_vec()
    } else {
        let block_size: usize = paths.iter().map(|(_, size)| size).sum();
        debug_assert!(obj_meta.size() as usize == block_size);

        let mut block_stream =
            BlockStream::new(paths, block_size, RangeRequest::All, metrics.to_cas());
        let mut data = Vec::with_capacity(block_size);

        while let Some(chunk_result) = block_stream.next().await {
            let chunk: Bytes = chunk_result?;
            data.extend_from_slice(&chunk);
        }
        data
    };

    Ok(Some(data))
}
