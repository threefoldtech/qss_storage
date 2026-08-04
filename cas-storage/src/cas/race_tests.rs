//! Race-stress and cancellation tests for the ADR 0006 block protocol
//! (plan component 9).
//!
//! These tests are written against the INVARIANTS, not the
//! implementation: never a record without a complete file, never an
//! unlinked live block, exact refcounts at quiesce. Iteration counts are
//! sized for CI; when hunting a suspected race, raise `STORM_ITERATIONS`
//! and run under `--release` locally.

use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures::stream;
use tempfile::tempdir;

use super::block_disk::{BlockDiskOps, RealDiskOps};
use super::byte_stream::AsyncByteStream;
use super::delete_path::release_blocks;
use super::fs::CasFS;
use super::shared_block_store::SharedBlockStore;
use crate::metastore::{BlockId, Durability};
use crate::metrics::SharedMetrics;
use crate::store_options::StoreOptions;

mod cancellation;
mod dedup_races;
mod rc_storms;

const STORM_ITERATIONS: usize = 60;

/// One store, `n` namespaces over it. Returns the shared store handle and
/// the namespaces.
fn store_with_namespaces(
    dir: &std::path::Path,
    ops: Option<Arc<dyn BlockDiskOps>>,
    n: usize,
) -> (Arc<SharedBlockStore>, Vec<Arc<CasFS>>) {
    let opts = StoreOptions {
        inline_metadata_size: Some(1),
        durability: Durability::Buffer,
        ..StoreOptions::default()
    };
    let mut shared =
        SharedBlockStore::new(dir.join("meta/blocks"), dir.join("blocks"), opts).unwrap();
    if let Some(ops) = ops {
        shared.set_disk_ops(ops);
    }
    let shared = Arc::new(shared);
    let namespaces = (0..n)
        .map(|i| {
            Arc::new(
                CasFS::new(
                    dir.join(format!("meta/ns-{i}")),
                    shared.clone(),
                    SharedMetrics::default(),
                    opts,
                )
                .unwrap(),
            )
        })
        .collect();
    (shared, namespaces)
}

fn byte_stream(data: Vec<u8>) -> (AsyncByteStream, usize) {
    let len = data.len();
    (
        AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(data)) })),
        len,
    )
}

async fn put(fs: &CasFS, bucket: &str, key: &str, data: Vec<u8>) {
    let (stream, len) = byte_stream(data);
    fs.store_single_object_and_meta(bucket, key, stream, len)
        .await
        .unwrap();
}

/// The invariant every release leg below shares: the record carries exactly
/// the expected count, and the block file exists IFF something still
/// references it. Anything else is a torn rc -- a leaked file under a dead
/// record, or a live record whose bytes were unlinked out from under it.
fn assert_block_state(
    shared: &SharedBlockStore,
    id: BlockId,
    expected_rc: usize,
    root: &std::path::Path,
) {
    match shared.block_tree().get_block(id.as_slice()).unwrap() {
        Some(block) => {
            assert_eq!(block.rc(), expected_rc, "exact rc at quiesce");
            assert!(expected_rc > 0, "a live record must carry a reference");
            let path = block.disk_path(&id, root.to_path_buf());
            let bytes = std::fs::read(&path).expect("a referenced block must keep its file");
            assert_eq!(
                shared.hasher().hash(&bytes),
                id,
                "the file is the block it is named after"
            );
        }
        None => {
            assert_eq!(
                expected_rc, 0,
                "the record vanished with references outstanding"
            );
            assert!(
                !crate::metastore::block_disk_path(&id, 1, root.to_path_buf()).exists(),
                "the last release must unlink the file"
            );
        }
    }
}
