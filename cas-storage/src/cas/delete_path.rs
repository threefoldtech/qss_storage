use faster_hex::hex_string;

use super::fs::CasFS;
use crate::metastore::MetaError;

#[tracing::instrument(skip(fs), fields(bucket = %bucket, key = %key, blocks_deleted))]
pub(super) async fn delete_object(fs: &CasFS, bucket: &str, key: &str) -> Result<(), MetaError> {
    let path_map = fs.path_tree()?;

    let block_tree = fs.shared.block_tree();
    let blocks_to_delete = fs.namespace.delete_object(bucket, key, &block_tree)?;

    tracing::Span::current().record("blocks_deleted", blocks_to_delete.len());

    for block in blocks_to_delete {
        async_fs::remove_file(block.disk_path(fs.fs_root().clone()))
            .await
            .expect("Could not delete file");
        if let Err(e) = path_map.remove(block.path()) {
            tracing::error!(
                path = %hex_string(block.path()),
                error = %e,
                "Could not unlink path from path map"
            );
        };
    }

    Ok(())
}

#[tracing::instrument(skip(fs), fields(bucket = %bucket_name, objects_deleted))]
pub(super) async fn bucket_delete(fs: &CasFS, bucket_name: &str) -> Result<(), MetaError> {
    let bmt = fs.namespace.get_allbuckets_tree()?;
    bmt.remove(bucket_name.as_bytes())?;

    let bucket = fs.namespace.get_bucket_ext(bucket_name)?;
    let mut object_count = 0;
    for key_val in bucket.iter_all() {
        let (key, _) = key_val?;
        delete_object(
            fs,
            bucket_name,
            std::str::from_utf8(&key).expect("keys are valid utf-8"),
        )
        .await?;
        object_count += 1;
    }

    tracing::Span::current().record("objects_deleted", object_count);

    fs.namespace.drop_bucket(bucket_name)?;
    Ok(())
}
