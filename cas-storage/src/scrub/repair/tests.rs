//! Repair tests: every action against the residue it exists for, plus the
//! three properties the ADR demands of the set as a whole -- idempotent,
//! re-runnable after a kill, and refusing every rc mutation when the holder
//! set was not closed.

use std::path::{Path, PathBuf};

use super::plan::flag_unfinished;
use super::*;
use crate::cas::AsyncByteStream;
use crate::cas::block_disk::QUARANTINE_DIR_NAME;
use crate::metastore::{Block, block_disk_path};
use crate::scrub::engine::{self, ScrubOptions};
use crate::scrub::findings::{Finding, FindingClass, Severity};
use crate::scrub::report::{Pass, Report, exit_code};
use crate::scrub::tests::{plant_object, put, store, synthetic_id};

mod blocks;
mod holders;
mod properties;

/// The tool's workflow: report first, repair second, check third.
async fn report_and_repair(fs: &CasFS, options: ScrubOptions) -> RepairSummary {
    let ctx = RepairContext::new(fs);
    let report = engine::run(&ctx.scrub_context(), &options).unwrap();
    repair(&ctx, &report, &options).await.unwrap()
}

/// The record for `id`, or `None` if there is none.
fn record(fs: &CasFS, id: BlockId) -> Option<Block> {
    fs.shared_block_store()
        .block_tree()
        .get_block(id.as_slice())
        .unwrap()
}

/// The rc the record for `id` states; panics if there is no record.
fn rc(fs: &CasFS, id: BlockId) -> usize {
    record(fs, id).expect("the record must exist").rc()
}

/// Where the record for `id` says its file is.
fn recorded_path(fs: &CasFS, id: BlockId) -> PathBuf {
    let depth = record(fs, id).expect("the record must exist").depth();
    block_disk_path(&id, depth, fs.fs_root().clone())
}

fn quarantine_dir(fs: &CasFS) -> PathBuf {
    fs.fs_root().join(QUARANTINE_DIR_NAME)
}

/// Moves a block file from one fanout depth to another: the placement-drift
/// shape that leaves a record pointing where its bytes no longer are.
fn misplace_block_file(root: &Path, id: &BlockId, from: u8, to: u8) {
    let source = block_disk_path(id, from, root.to_path_buf());
    let dest = block_disk_path(id, to, root.to_path_buf());
    std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
    std::fs::rename(&source, &dest).unwrap();
}

/// Stores one part the way `UploadPart` does -- the blocks first, which
/// bumps their refcounts, then the part record -- and returns its block
/// ids. Without an upload record for the same triple this is an ORPHAN
/// part: blocks held by a record nothing can complete or abort.
async fn put_part(
    fs: &CasFS,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i64,
    data: Vec<u8>,
) -> Vec<BlockId> {
    let stream = AsyncByteStream::new(futures::stream::once(async move {
        Ok(bytes::Bytes::from(data))
    }));
    let (blocks, hash, size) = fs.store_object(bucket, key, stream).await.unwrap();
    fs.insert_multipart_part(
        bucket.to_string(),
        key.to_string(),
        size as usize,
        part_number,
        upload_id.to_string(),
        hash,
        blocks.clone(),
    )
    .unwrap();
    blocks
}

/// The outcomes of one kind, for asserting on what an action said.
fn of_kind<'a>(summary: &'a RepairSummary, kind: &str) -> Vec<&'a RepairOutcome> {
    summary
        .outcomes
        .iter()
        .filter(|outcome| outcome.action == kind)
        .collect()
}
