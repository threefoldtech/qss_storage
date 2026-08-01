//! What an ack promises the kernel, pinned by reading the journal off disk.
//!
//! The contract at `buffer` durability is that an acked write survives the
//! PROCESS dying and is lost only to a power cut or an OS crash. That is a
//! claim about `write(2)`, not about `fsync(2)`, and it is therefore
//! observable in-process with no crash machinery at all: open the store's
//! journal files through the filesystem and look for the record's bytes. If
//! they are there, a `kill -9` cannot take them; if they are not, they live in
//! a userspace buffer that dies with the process.
//!
//! That is exactly how the campaign's buffer-loss corpse was read
//! (`target/realtest/loss-snapshots-20260801T144750/buffer-cycle7`): the acked
//! object key was nowhere in the namespace journal file while its neighbours
//! were. These tests are that forensic one-liner, run before the ack instead
//! of after the kill -- once per shape a client can be acknowledged for.
//!
//! They pass on both sides of the ADR 0011 rider, and that is a finding
//! rather than a defect in them: fjall 3.1.8 flushes its journal buffer on
//! every write path we use, so `write` visibility was never the hole. What
//! the rider fixed is the level ABOVE it -- see the rider section of ADR
//! 0011. These stay as the regression pin, because the property they assert
//! is one nobody would notice losing until a kill test lost an object.

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use tempfile::tempdir;

use super::byte_stream::AsyncByteStream;
use super::fs::CasFS;
use super::shared_block_store::SharedBlockStore;
use crate::metastore::{ContentHash, Durability, ObjectData};
use crate::metrics::SharedMetrics;

const BUCKET: &str = "acks";

/// One store at `durability`, with the namespace DB at `<dir>/meta/db` and the
/// blocks DB at `<dir>/blocks/.db`.
fn store(dir: &Path, durability: Durability) -> (Arc<SharedBlockStore>, CasFS) {
    let shared = Arc::new(
        SharedBlockStore::new(
            dir.join("meta/blocks"),
            dir.join("blocks"),
            super::StorageEngine::Fjall,
            Some(1),
            Some(durability),
            None,
            None,
            None,
            None,
        )
        .unwrap(),
    );
    let fs = CasFS::new(
        dir.join("meta"),
        shared.clone(),
        SharedMetrics::default(),
        super::StorageEngine::Fjall,
        Some(1),
        Some(durability),
        false,
    )
    .unwrap();
    fs.create_bucket(BUCKET).unwrap();
    (shared, fs)
}

/// Every byte of every journal file under `db_dir`, read through the
/// filesystem.
///
/// Read with `std::fs`, deliberately: this must see what the KERNEL has, not
/// what the process thinks it wrote. A journal entry still sitting in fjall's
/// `BufWriter` is not in here, which is the whole point.
fn journal_bytes(db_dir: &Path) -> Vec<u8> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(db_dir).expect("the database directory must exist") {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|ext| ext == "jnl") {
            out.extend_from_slice(&std::fs::read(&path).unwrap());
        }
    }
    assert!(
        !out.is_empty(),
        "no journal bytes at all under {}",
        db_dir.display()
    );
    out
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

async fn put(fs: &CasFS, key: &str, data: Vec<u8>) {
    let len = data.len();
    let stream = AsyncByteStream::new(futures::stream::once(async move { Ok(Bytes::from(data)) }));
    fs.store_single_object_and_meta(BUCKET, key, stream, len)
        .await
        .unwrap();
}

/// A PUT that returned success has its object record in the namespace
/// journal, on disk, at BOTH durability levels.
///
/// This is the buffer contract as a test. At `buffer` the record's bytes must
/// have reached the kernel -- no fsync, just `write` -- so that a `kill -9`
/// cannot take back what the client was told. At `fsync` the same must hold a
/// fortiori.
#[tokio::test]
async fn an_acked_put_has_its_object_record_in_the_namespace_journal() {
    for durability in [Durability::Buffer, Durability::Fsync] {
        let dir = tempdir().unwrap();
        let (_shared, fs) = store(dir.path(), durability);

        put(&fs, "the-acked-key", b"payload".repeat(64).to_vec()).await;

        let journal = journal_bytes(&dir.path().join("meta/db"));
        assert!(
            contains(&journal, b"the-acked-key"),
            "{durability}: the acked object record is not in the namespace \
             journal on disk -- it is in a userspace buffer a kill would take"
        );
    }
}

/// The same for a multipart complete, which is the shape the campaign lost.
///
/// `CompleteMultipartUpload` acks an ETag; the object record behind it has to
/// be as durable as that ack claims. Findings buffer-3/mp-94 and mp-61, and
/// phase-7 cycle-7 buffer-7/mp-68, were all this record going missing across a
/// kill at `buffer`.
#[tokio::test]
async fn an_acked_multipart_complete_has_its_records_in_the_journals() {
    for durability in [Durability::Buffer, Durability::Fsync] {
        let dir = tempdir().unwrap();
        let (shared, fs) = store(dir.path(), durability);

        let upload_id = "upload-under-test";
        let key = "the-completed-key";
        fs.create_upload(BUCKET, key, upload_id).unwrap();

        // The part record: written and acked with an ETag by UploadPart,
        // which makes it an ack-carrying write of its own.
        let data = b"a part's worth of bytes".repeat(64).to_vec();
        let id = shared.hasher().hash(&data);
        let len = data.len();
        let stream =
            AsyncByteStream::new(futures::stream::once(async move { Ok(Bytes::from(data)) }));
        let (blocks, hash, size) = fs.store_object(BUCKET, key, stream).await.unwrap();
        assert_eq!(blocks, vec![id]);
        fs.insert_multipart_part(
            BUCKET.to_string(),
            key.to_string(),
            len,
            1,
            upload_id.to_string(),
            hash,
            blocks.clone(),
        )
        .unwrap();

        let blocks_journal = journal_bytes(&dir.path().join("meta/blocks/.db"));
        assert!(
            contains(&blocks_journal, key.as_bytes()),
            "{durability}: the acked part record is not in the blocks journal \
             on disk"
        );

        // The complete: claim the upload and its parts, then write the object
        // record that the ETag the client receives stands for.
        let claimed = fs
            .claim_upload_with_parts(BUCKET, key, upload_id, &[1])
            .unwrap();
        assert!(matches!(claimed, crate::cas::UploadClaim::Claimed { .. }));
        fs.create_object_meta(
            BUCKET,
            key,
            size,
            ContentHash::from([3u8; 16]),
            ObjectData::MultiPart { blocks, parts: 1 },
        )
        .await
        .unwrap();

        let journal = journal_bytes(&dir.path().join("meta/db"));
        assert!(
            contains(&journal, key.as_bytes()),
            "{durability}: the acked complete's object record is not in the \
             namespace journal on disk -- the shape of finding buffer-7/mp-68"
        );
    }
}

/// A created bucket and a created upload are acked too, and both are written
/// through the non-transactional tree surface.
///
/// Not the finding's shape, but the same hole: a client that got a 200 from
/// `CreateBucket` or `CreateMultipartUpload` and then watched the daemon die
/// must not find them gone.
#[tokio::test]
async fn acked_bucket_and_upload_records_reach_the_kernel() {
    for durability in [Durability::Buffer, Durability::Fsync] {
        let dir = tempdir().unwrap();
        let (_shared, fs) = store(dir.path(), durability);

        fs.create_bucket("a-second-bucket").unwrap();
        assert!(
            contains(
                &journal_bytes(&dir.path().join("meta/db")),
                b"a-second-bucket"
            ),
            "{durability}: an acked CreateBucket must be on the kernel's side \
             of the buffer"
        );

        fs.create_upload(BUCKET, "some/key", "the-upload-id")
            .unwrap();
        assert!(
            contains(
                &journal_bytes(&dir.path().join("meta/blocks/.db")),
                b"the-upload-id"
            ),
            "{durability}: an acked CreateMultipartUpload must be on the \
             kernel's side of the buffer"
        );
    }
}
