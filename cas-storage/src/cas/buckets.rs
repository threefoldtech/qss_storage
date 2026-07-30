use std::sync::Arc;

use super::fs::CasFS;
use crate::metastore::{BucketMeta, MetaError, MetaTreeExt};

pub(super) fn create_bucket(fs: &CasFS, bucket_name: &str) -> Result<(), MetaError> {
    let bm = BucketMeta::new(bucket_name.to_string());
    fs.namespace.insert_bucket(bucket_name, bm.to_vec())
}

pub(super) fn list_buckets(fs: &CasFS) -> Result<Vec<BucketMeta>, MetaError> {
    fs.namespace.list_buckets()
}

pub(super) fn bucket_exists(fs: &CasFS, bucket_name: &str) -> Result<bool, MetaError> {
    fs.namespace.bucket_exists(bucket_name)
}

pub(super) fn get_bucket(
    fs: &CasFS,
    bucket_name: &str,
) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError> {
    fs.namespace.get_bucket_ext(bucket_name)
}

pub(super) fn key_exists(fs: &CasFS, bucket: &str, key: &str) -> Result<bool, MetaError> {
    let bucket = get_bucket(fs, bucket)?;
    bucket.contains_key(key.as_bytes())
}
