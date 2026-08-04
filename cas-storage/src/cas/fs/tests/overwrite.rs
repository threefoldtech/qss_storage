use super::*;

/// An INLINE write replacing a BLOCK-BACKED object: the case ADR 0008
/// singles out. The new record names no blocks at all, so the release is
/// the only thing standing between the overwrite and a permanent leak --
/// nothing that survives could ever name those references again.
#[tokio::test]
async fn inline_over_block_backed_releases_the_replaced_blocks() {
    for (engine, hasher) in matrix() {
        let (fs, _dir) = setup_test_fs(engine, hasher);
        fs.create_bucket("b").unwrap();

        let obj = put_blocks(&fs, "b", "k", b"block backed payload".repeat(60).to_vec()).await;
        let id = obj.blocks()[0];
        assert_eq!(rc_of(&fs, &id), Some(1));

        fs.store_inlined_object("b", "k", b"now inline".to_vec())
            .await
            .unwrap();

        assert_eq!(
            rc_of(&fs, &id),
            None,
            "the inline record names nothing, so the last reference is gone"
        );
        assert!(
            !crate::metastore::block_disk_path(&id, 1, fs.fs_root().clone()).exists(),
            "the last release must unlink the file"
        );
        assert_eq!(
            fs.get_object_meta("b", "k").unwrap().unwrap().inlined(),
            Some(&b"now inline".to_vec()),
            "the inline object is the one that survives"
        );
    }
}

/// Inline over inline: neither record holds a reference, so there is
/// nothing to release and the overwrite is a plain record replacement.
/// This is the shape respcas's `set` has (through its own tree, not this
/// path -- respcas carries no block store at all).
#[tokio::test]
async fn inline_over_inline_releases_nothing() {
    let (fs, _dir) = setup_test_fs(StorageEngine::Fjall, Hasher::Blake3W32);
    fs.create_bucket("b").unwrap();

    fs.store_inlined_object("b", "k", b"first".to_vec())
        .await
        .unwrap();
    fs.store_inlined_object("b", "k", b"second".to_vec())
        .await
        .unwrap();

    assert_eq!(
        fs.get_object_meta("b", "k").unwrap().unwrap().inlined(),
        Some(&b"second".to_vec())
    );
    assert_eq!(
        fs.shared.block_tree().len().unwrap(),
        0,
        "an inline object never touched a block record"
    );
}

/// Block-backed over inline: the displaced record holds no references,
/// so the release is a no-op and the new object's block sits at exactly
/// one.
#[tokio::test]
async fn block_backed_over_inline_releases_nothing() {
    let (fs, _dir) = setup_test_fs(StorageEngine::Fjall, Hasher::Blake3W32);
    fs.create_bucket("b").unwrap();

    fs.store_inlined_object("b", "k", b"inline first".to_vec())
        .await
        .unwrap();
    let obj = put_blocks(&fs, "b", "k", b"blocks second".repeat(60).to_vec()).await;

    assert_eq!(rc_of(&fs, &obj.blocks()[0]), Some(1));
}

/// An overwrite with DIFFERENT content: the old block loses its only
/// holder and goes, the new one arrives at one. The bump-and-release
/// arithmetic has no shared block to cancel against here, so both
/// directions are visible in one test.
#[tokio::test]
async fn overwrite_with_new_content_drops_the_old_block_and_keeps_the_new() {
    let (fs, _dir) = setup_test_fs(StorageEngine::Fjall, Hasher::Blake3W32);
    fs.create_bucket("b").unwrap();

    let old = put_blocks(&fs, "b", "k", b"the old bytes".repeat(60).to_vec()).await;
    let new = put_blocks(&fs, "b", "k", b"the new bytes".repeat(60).to_vec()).await;
    let (old_id, new_id) = (old.blocks()[0], new.blocks()[0]);
    assert_ne!(old_id, new_id, "the test needs distinct content");

    assert_eq!(rc_of(&fs, &old_id), None, "-1 for the dropped block");
    assert!(!crate::metastore::block_disk_path(&old_id, 1, fs.fs_root().clone()).exists());
    assert_eq!(rc_of(&fs, &new_id), Some(1), "+1 for the added block");
}

/// An overwrite whose new object SHARES a block with the one it replaces
/// nets to no change: the write bumped the shared block (every dedup hit
/// bumps, ADR 0006) and the release dropped the old occurrence. The
/// unshared halves move by exactly one each.
#[tokio::test]
async fn overwrite_sharing_a_block_nets_to_no_change() {
    let (fs, _dir) = setup_test_fs(StorageEngine::Fjall, Hasher::Blake3W32);
    fs.create_bucket("b").unwrap();

    // Two objects of two blocks each, sharing their first block. The
    // block size is 1 MiB, so a chunk boundary needs that much data.
    let shared_half = b"shared prefix block ".repeat(60_000);
    let old_half = b"old suffix block ".repeat(60_000);
    let new_half = b"new suffix block ".repeat(60_000);

    let old = put_blocks(
        &fs,
        "b",
        "k",
        [shared_half.clone(), old_half].concat().to_vec(),
    )
    .await;
    assert!(old.blocks().len() > 1, "the fixture needs two blocks");
    let shared_id = old.blocks()[0];
    let old_tail = *old.blocks().last().unwrap();
    assert_eq!(rc_of(&fs, &shared_id), Some(1));

    let new = put_blocks(&fs, "b", "k", [shared_half, new_half].concat().to_vec()).await;
    assert_eq!(
        new.blocks()[0],
        shared_id,
        "the fixture needs a shared head"
    );
    let new_tail = *new.blocks().last().unwrap();
    assert_ne!(new_tail, old_tail, "the fixture needs a changed tail");

    assert_eq!(
        rc_of(&fs, &shared_id),
        Some(1),
        "bump and release cancel on the shared block"
    );
    assert_eq!(
        rc_of(&fs, &old_tail),
        None,
        "the dropped tail loses its one"
    );
    assert_eq!(rc_of(&fs, &new_tail), Some(1), "the added tail gains one");
}
