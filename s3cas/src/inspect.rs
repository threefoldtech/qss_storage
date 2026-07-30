use anyhow::Result;
use std::path::PathBuf;

use crate::store_options::StoreOptions;
use cas_storage::{FjallStore, FjallStoreNotx, MetaStore, StorageEngine};

/// Opens the metadata store at `meta_root` through the header-aware path, so
/// the tools refuse a store this build may not read for the same reasons the
/// server does.
///
/// Durability, inline threshold and header hash all come from `store`, which
/// is the merge of the CLI flags and the config file. A tool that opened the
/// store with settings other than the ones the server runs with would report
/// on a store nobody is running.
///
/// Note that `meta_root` is used as given: unlike `CasFS`, this does not
/// append `db/`. That mismatch is a known bug, fixed with the rest of the tool
/// UX in a later step.
fn open(meta_root: PathBuf, store: &StoreOptions) -> Result<MetaStore> {
    let inline = store.inline_metadata_size;
    let spec = store.header_spec();
    let (meta_store, _header) = match store.metadata_db {
        StorageEngine::Fjall => {
            let durability = Some(store.durability);
            MetaStore::open_or_create(meta_root, inline, spec, |p| {
                FjallStore::new(p, inline, durability)
            })?
        }
        StorageEngine::FjallNotx => {
            MetaStore::open_or_create(meta_root, inline, spec, |p| FjallStoreNotx::new(p, inline))?
        }
    };
    Ok(meta_store)
}

pub fn num_keys(meta_root: PathBuf, store: &StoreOptions, bucket_name: &str) -> Result<usize> {
    let meta_store = open(meta_root, store)?;

    // Per-tree key count is not on MetaStore; reach into the underlying Store.
    let bucket_keys = meta_store.get_underlying_store().num_keys(bucket_name)?;
    Ok(bucket_keys)
}

pub fn disk_space(meta_root: PathBuf, store: &StoreOptions) -> Result<u64> {
    Ok(open(meta_root, store)?.disk_space())
}
