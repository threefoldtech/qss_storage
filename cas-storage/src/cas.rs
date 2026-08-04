pub mod block_stream;
pub mod gc;
pub mod multipart;
pub mod range_request;
pub mod shared_block_store;
pub use block_disk::{BLOCKS_DB_DIR_NAME, STORE_ID_MARKER_NAME};
pub use fs::CasFS;
pub use fs::StorageEngine;
pub use gc::{SweepStats, sweep_stale_uploads};
pub use group_commit::{GroupCommit, GroupCommitStats};
pub use shared_block_store::SharedBlockStore;
pub use uploads::UploadClaim;
pub(crate) mod block_disk;
mod buckets;
mod buffered_byte_stream;
pub mod byte_stream;
mod delete_path;
pub mod fs;
pub(crate) mod group_commit;
mod placement;
mod read_path;
pub(crate) mod stripes;
mod uploads;
pub(crate) mod write_path;

#[cfg(test)]
mod ack_visibility_tests;
#[cfg(test)]
pub(crate) mod crash_fixtures;
#[cfg(test)]
mod race_tests;

pub use byte_stream::AsyncByteStream;

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
