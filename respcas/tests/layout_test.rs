//! The store on disk: the layout ADR 0014 gave respcas, and the one it
//! keeps opening.

mod common;

use cas_storage::{CasFS, SharedMetrics, StoreOptions};
use common::{rewind_to_pre_0014_layout, store_options};
use respcas::storage::Storage;
use tempfile::tempdir;

/// A store this build creates is the standard pair every tool walks:
/// the header sidecar, the metadata database under db/, and the block store
/// under blocks/.
#[test]
fn a_new_store_has_the_layout_the_tools_expect() {
    let dir = tempdir().unwrap();
    let root = dir.path();

    {
        let storage = Storage::new(root.to_path_buf(), store_options()).expect("a store is made");
        storage.init_namespace().expect("the default namespace");
    }

    assert!(
        root.join("db").is_dir(),
        "the metadata database is under db/"
    );
    assert!(
        root.join("blocks").is_dir(),
        "and the block store beside it"
    );
    assert!(
        root.join("blocks").join(".db").is_dir(),
        "with the blocks database where SharedBlockStore puts it"
    );
    assert!(
        root.join("store_header.bin").is_file(),
        "the sidecar copy of the header lands in the data directory"
    );
}

/// A store created before ADR 0014 has fjall's files straight in the data
/// directory. It opens there, unchanged and unmoved, and gains its blocks/
/// directory additively -- that is what "existing stores keep working" has
/// to mean when the layout around them changes.
#[test]
fn a_store_from_before_the_layout_opens_where_it_is() {
    let dir = tempdir().unwrap();
    let root = dir.path();

    // Build a store, then rewind it to the pre-0014 shape: database in the
    // data directory itself, no blocks/ anywhere.
    {
        let storage = Storage::new(root.to_path_buf(), store_options()).expect("a store is made");
        storage.init_namespace().expect("the default namespace");
        storage
            .create_namespace("legacy-ns")
            .expect("a namespace to find again afterwards");
    }
    rewind_to_pre_0014_layout(root);

    // It opens, and what was in it is still in it.
    let storage = Storage::new(root.to_path_buf(), store_options())
        .expect("a store from before the layout must still open");
    storage
        .get_namespace_meta("legacy-ns")
        .expect("the namespaces are where they were");

    // Nothing moved.
    assert!(!root.join("db").exists(), "no migration happened");
    assert!(root.join("version").is_file());

    // And the block store appeared, so the daemon can serve a
    // content-addressed namespace out of a store that predates them.
    assert!(
        root.join("blocks").join(".db").is_dir(),
        "blocks/ is created additively on first open"
    );

    // A second open finds the same store the same way.
    drop(storage);
    let reopened = Storage::new(root.to_path_buf(), store_options()).expect("and it reopens");
    reopened
        .get_namespace_meta("legacy-ns")
        .expect("still there");
}

/// The point of the layout: fsck can walk a respcas store, mixed key modes
/// and all (ADR 0014). The daemon must be stopped first -- fjall holds a
/// directory lock, which is the exclusivity fsck relies on.
#[tokio::test]
async fn fsck_walks_a_store_with_both_kinds_of_namespace() {
    use cas_storage::scrub::{ScrubContext, ScrubOptions};

    let dir = tempdir().unwrap();
    let root = dir.path();

    {
        let storage = Storage::new(root.to_path_buf(), store_options()).expect("a store is made");
        storage.init_namespace().unwrap();

        // A user-keyed namespace with an inline record, and a
        // content-addressed one with a block-backed record.
        let named = storage.create_namespace("named").unwrap();
        named
            .insert(
                b"a-user-key",
                cas_storage::Object::new(
                    5,
                    cas_storage::ContentHash::from([0u8; 16]),
                    cas_storage::ObjectData::Inline {
                        data: b"hello".to_vec(),
                    },
                )
                .to_vec(),
            )
            .unwrap();

        storage.create_namespace("addressed").unwrap();
        storage
            .set_key_mode("addressed", respcas::storage::KeyMode::Cas)
            .unwrap();

        let value = vec![0x7fu8; 3 << 20];
        let key = respcas::content::value_key(&value);
        let stream = cas_storage::AsyncByteStream::new(futures::stream::once(async move {
            Ok(bytes::Bytes::from(value))
        }));
        let (blocks, hash, size) = storage
            .cas()
            .store_object("addressed", key, stream)
            .await
            .unwrap();
        storage
            .cas()
            .create_object_meta(
                "addressed",
                key,
                size,
                hash,
                cas_storage::ObjectData::SinglePart { blocks },
            )
            .await
            .unwrap();
    }

    // Now open it the way fsck does -- one root for both halves -- and walk
    // it. A clean store must report nothing.
    let casfs = CasFS::single_namespace(
        root.to_path_buf(),
        root.to_path_buf(),
        SharedMetrics::default(),
        StoreOptions {
            verify_on_read: false,
            ..store_options()
        },
    )
    .expect("fsck must be able to open a respcas store");

    let ctx = ScrubContext::new(casfs.namespace_meta_store(), casfs.shared_block_store())
        .with_meta_root(root.to_path_buf());
    let report = cas_storage::scrub::run(&ctx, &ScrubOptions::full()).expect("the walk must run");

    assert!(
        report.findings.is_empty(),
        "a healthy mixed-mode store has nothing to report: {:#?}",
        report.findings
    );
}
