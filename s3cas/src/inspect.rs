use anyhow::Result;
use std::path::PathBuf;

use cas_storage::{FjallStore, FjallStoreNotx, HeaderSpec, MetaStore, StorageEngine};

/// Opens the metadata store at `meta_root` through the header-aware path, so
/// the tools refuse a store this build may not read for the same reasons the
/// server does.
///
/// Note that `meta_root` is used as given: unlike `CasFS`, this does not
/// append `db/`. That mismatch is a known bug, fixed with the rest of the tool
/// UX in a later step.
fn open(meta_root: PathBuf, storage_engine: StorageEngine) -> Result<MetaStore> {
    let (meta_store, _header) = match storage_engine {
        StorageEngine::Fjall => {
            MetaStore::open_or_create(meta_root, None, HeaderSpec::default(), |p| {
                FjallStore::new(p, None, None)
            })?
        }
        StorageEngine::FjallNotx => {
            MetaStore::open_or_create(meta_root, None, HeaderSpec::default(), |p| {
                FjallStoreNotx::new(p, None)
            })?
        }
    };
    Ok(meta_store)
}

pub fn num_keys(
    meta_root: PathBuf,
    storage_engine: StorageEngine,
    bucket_name: &str,
) -> Result<usize> {
    let meta_store = open(meta_root, storage_engine)?;

    // Per-tree key count is not on MetaStore; reach into the underlying Store.
    let bucket_keys = meta_store.get_underlying_store().num_keys(bucket_name)?;
    Ok(bucket_keys)
}

pub fn disk_space(meta_root: PathBuf, storage_engine: StorageEngine) -> Result<u64> {
    Ok(open(meta_root, storage_engine)?.disk_space())
}
