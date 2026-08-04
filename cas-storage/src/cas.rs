pub(crate) mod block_disk;
mod block_stream;
mod buckets;
mod buffered_byte_stream;
mod byte_stream;
mod clone_path;
mod delete_path;
/// The one module of the pipeline that stays public, for one item the
/// re-exports below do not carry: s3cas's check tests size their objects in
/// whole blocks with `cas::fs::BLOCK_SIZE`.
pub mod fs;
mod gc;
pub(crate) mod group_commit;
pub(crate) mod multipart;
mod placement;
mod range_request;
mod read_path;
mod shared_block_store;
pub(crate) mod stripes;
mod uploads;
pub(crate) mod write_path;

#[cfg(test)]
mod ack_visibility_tests;
#[cfg(test)]
pub(crate) mod crash_fixtures;
#[cfg(test)]
mod race_tests;

pub use block_disk::{BLOCKS_DB_DIR_NAME, STORE_ID_MARKER_NAME};
pub use block_stream::{BlockCorruption, BlockStream};
pub use byte_stream::AsyncByteStream;
pub use fs::{CasFS, StorageEngine};
pub use gc::{SweepStats, sweep_stale_uploads};
pub use group_commit::{GroupCommit, GroupCommitStats};
pub use multipart::{MultiPart, MultiPartTree};
pub use range_request::RangeRequest;
pub use shared_block_store::SharedBlockStore;
pub use uploads::UploadClaim;

/// An object key as a log line should show it.
///
/// Object keys are bytes here, not text: an S3 key is UTF-8 by the API's own
/// rules, but a respcas Cas namespace files its records under the raw BLAKE3
/// of the value (ADR 0014), which is not. So a key renders as itself when it
/// is text and as hex when it is not, and neither case produces the
/// replacement characters a lossy decode would put in a log.
pub(crate) fn object_key(key: &[u8]) -> std::borrow::Cow<'_, str> {
    match std::str::from_utf8(key) {
        Ok(text) => std::borrow::Cow::Borrowed(text),
        Err(_) => std::borrow::Cow::Owned(faster_hex::hex_string(key)),
    }
}
