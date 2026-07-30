use super::fs::{CasFS, ObjectPaths};
use crate::metastore::{MetaError, Object};

pub(super) fn get_object_meta(
    fs: &CasFS,
    bucket_name: &str,
    key: &str,
) -> Result<Option<Object>, MetaError> {
    fs.namespace.get_meta(bucket_name, key)
}

pub(super) fn get_object_paths(
    fs: &CasFS,
    bucket_name: &str,
    key: &str,
) -> Result<Option<ObjectPaths>, MetaError> {
    let Some(obj_meta) = get_object_meta(fs, bucket_name, key)? else {
        return Ok(None);
    };

    if obj_meta.is_inlined() {
        return Ok(Some((obj_meta, vec![])));
    }

    let blocks = obj_meta.blocks();
    let block_map = fs.block_tree()?;
    let mut paths = Vec::with_capacity(blocks.len());
    for block in blocks {
        let block_meta = block_map
            .get_block(block)?
            .ok_or(MetaError::BlockNotFound)?;
        paths.push((
            block_meta.disk_path(fs.fs_root().clone()),
            block_meta.size(),
        ));
    }
    Ok(Some((obj_meta, paths)))
}
