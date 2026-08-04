use super::*;

/// One release racing K concurrent PUTs of the same content. The release's
/// striped decrement and each PUT's striped bump serialize on the one
/// stripe, so the arithmetic is exact whatever the interleaving: one seed
/// reference, plus K, minus the one released, is K.
///
/// The extreme interleaving is the interesting one: if the release lands
/// first it takes the LAST reference, removing the record and unlinking the
/// file inside its stripe hold -- and the PUTs that follow must then rebuild
/// the block from rc 1 rather than resurrecting a half-dead record.
///
/// (The seed PUT stands in for whichever record held that reference; a real
/// caller -- abort, from component 4 -- removes its part record before
/// calling. This arm pins the rc protocol, not the caller's ordering.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn release_blocks_vs_concurrent_puts_keeps_exact_rc() {
    /// Concurrent writers of the same content, one per namespace.
    const K: usize = 8;

    let dir = tempdir().unwrap();
    let (shared, namespaces) = store_with_namespaces(dir.path(), None, K);
    let metrics = SharedMetrics::default();
    for fs in &namespaces {
        fs.create_bucket("b").unwrap();
    }

    for iteration in 0..STORM_ITERATIONS {
        // Fresh content per iteration: each one is a fresh race.
        let data = format!("release race {iteration} ").repeat(64).into_bytes();
        let id = shared.hasher().hash(&data);

        // Seed the reference the release will drop.
        put(
            &namespaces[0],
            "b",
            &format!("seed-{iteration}"),
            data.clone(),
        )
        .await;

        let mut tasks = Vec::new();
        {
            let shared = shared.clone();
            let metrics = metrics.clone();
            tasks.push(tokio::spawn(async move {
                release_blocks(&shared, &metrics, &[id]).await;
            }));
        }
        for (i, fs) in namespaces.iter().enumerate() {
            let fs = fs.clone();
            let data = data.clone();
            tasks.push(tokio::spawn(async move {
                put(&fs, "b", &format!("k-{iteration}-{i}"), data).await;
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }

        assert_block_state(&shared, id, K, namespaces[0].fs_root());
    }
}

/// The same arithmetic in both orders, run sequentially so the interleaving
/// is not left to the scheduler: release-then-PUTs and PUTs-then-release
/// must land on the identical final state. The first order also pins the
/// rc-zero side of the invariant -- releasing the last reference removes the
/// record AND unlinks the file, before the PUTs rebuild it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn release_blocks_lands_the_same_state_in_either_order() {
    const K: usize = 4;

    let dir = tempdir().unwrap();
    let (shared, namespaces) = store_with_namespaces(dir.path(), None, K);
    let metrics = SharedMetrics::default();
    for fs in &namespaces {
        fs.create_bucket("b").unwrap();
    }

    for release_first in [true, false] {
        let leg = if release_first {
            "release-first"
        } else {
            "puts-first"
        };
        let data = format!("order {leg} ").repeat(64).into_bytes();
        let id = shared.hasher().hash(&data);

        put(&namespaces[0], "b", &format!("seed-{leg}"), data.clone()).await;
        assert_block_state(&shared, id, 1, namespaces[0].fs_root());

        if release_first {
            release_blocks(&shared, &metrics, &[id]).await;
            // That was the last reference: nothing left, file included.
            assert_block_state(&shared, id, 0, namespaces[0].fs_root());
        }

        for (i, fs) in namespaces.iter().enumerate() {
            put(fs, "b", &format!("k-{leg}-{i}"), data.clone()).await;
        }

        if !release_first {
            release_blocks(&shared, &metrics, &[id]).await;
        }

        assert_block_state(&shared, id, K, namespaces[0].fs_root());
    }
}

/// An overwrite storm on ONE key with the SAME content (ADR 0008).
///
/// Two rules meet on every write and cancel: each dedup hit bumps the shared
/// block (ADR 0006) and each overwrite releases the record it displaced. The
/// transactions serialize, so whatever the interleaving the storm is a chain
/// -- each writer displaces exactly the record before it and releases
/// exactly that -- and the count lands on the survivor's occurrences.
///
/// The pinned sibling reference makes both failure directions visible in one
/// assertion: a release that never happened shows up as an rc above the
/// truth, a release that happened twice as a record freed while the pinned
/// object still names it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overwrite_storm_on_one_key_keeps_exact_rc() {
    /// Concurrent overwriters of one key.
    const K: usize = 8;

    let dir = tempdir().unwrap();
    let (shared, namespaces) = store_with_namespaces(dir.path(), None, 1);
    let fs = namespaces[0].clone();
    fs.create_bucket("b").unwrap();

    let data = b"one key, many overwrites".repeat(100).to_vec();
    let id = shared.hasher().hash(&data);

    // The reference the storm never displaces.
    put(&fs, "b", "pinned", data.clone()).await;
    assert_block_state(&shared, id, 1, fs.fs_root());

    let mut tasks = Vec::new();
    for _ in 0..K {
        let fs = fs.clone();
        let data = data.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..STORM_ITERATIONS {
                put(&fs, "b", "k", data.clone()).await;
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    // K * STORM_ITERATIONS writes of one key leave exactly one object, and
    // exactly its occurrences on top of the pinned one.
    assert_block_state(&shared, id, 2, fs.fs_root());

    // And the whole lifecycle closes: no residue for fsck to collect.
    fs.delete_object("b", "k").await.unwrap();
    assert_block_state(&shared, id, 1, fs.fs_root());
    fs.delete_object("b", "pinned").await.unwrap();
    assert_block_state(&shared, id, 0, fs.fs_root());
}

/// The same storm with DISTINCT content per writer, where every overwrite
/// takes a block's last reference rather than cancelling against a bump.
///
/// Exactly one object survives each round, holding exactly one reference;
/// every content it replaced is gone, record and file. A missed release
/// leaves a record behind, a double release takes the survivor's own block
/// -- the round asserts against both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_overwrite_storm_of_distinct_content_leaves_only_the_survivor() {
    /// Concurrent overwriters, each with its own content.
    const K: usize = 6;

    let dir = tempdir().unwrap();
    let (shared, namespaces) = store_with_namespaces(dir.path(), None, 1);
    let fs = namespaces[0].clone();
    fs.create_bucket("b").unwrap();

    for iteration in 0..STORM_ITERATIONS {
        let contents: Vec<Vec<u8>> = (0..K)
            .map(|writer| {
                format!("overwrite {iteration}-{writer} ")
                    .repeat(64)
                    .into_bytes()
            })
            .collect();
        let ids: Vec<BlockId> = contents.iter().map(|c| shared.hasher().hash(c)).collect();

        let mut tasks = Vec::new();
        for data in contents.iter().cloned() {
            let fs = fs.clone();
            tasks.push(tokio::spawn(async move { put(&fs, "b", "k", data).await }));
        }
        for t in tasks {
            t.await.unwrap();
        }

        let survivor = fs
            .get_object_meta("b", "k")
            .unwrap()
            .expect("one object always survives an overwrite storm");
        assert_eq!(survivor.blocks().len(), 1, "single-block fixture");
        let alive = survivor.blocks()[0];
        for id in &ids {
            assert_block_state(&shared, *id, usize::from(*id == alive), fs.fs_root());
        }

        // The survivor's own reference goes the same way, leaving the store
        // clean for the next round.
        fs.delete_object("b", "k").await.unwrap();
        for id in &ids {
            assert_block_state(&shared, *id, 0, fs.fs_root());
        }
    }
}

/// Overwrite racing DELETE of one key.
///
/// Both operations take the record they act on out of a single transaction
/// -- `take_object` for the delete, `replace_object` for the overwrite -- so
/// whichever order they land in, each record is released exactly once by
/// exactly the caller that removed it. No new analysis: this is take/replace
/// symmetry, and the pinned reference is what would catch it failing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overwrite_racing_delete_releases_each_record_once() {
    let dir = tempdir().unwrap();
    let (shared, namespaces) = store_with_namespaces(dir.path(), None, 1);
    let fs = namespaces[0].clone();
    fs.create_bucket("b").unwrap();

    let data = b"overwritten and deleted".repeat(120).to_vec();
    let id = shared.hasher().hash(&data);
    put(&fs, "b", "pinned", data.clone()).await;

    let overwriter = {
        let fs = fs.clone();
        let data = data.clone();
        tokio::spawn(async move {
            for _ in 0..STORM_ITERATIONS {
                put(&fs, "b", "k", data.clone()).await;
            }
        })
    };
    let deleter = {
        let fs = fs.clone();
        tokio::spawn(async move {
            for _ in 0..STORM_ITERATIONS {
                fs.delete_object("b", "k").await.unwrap();
            }
        })
    };
    overwriter.await.unwrap();
    deleter.await.unwrap();

    // Whatever the interleaving left behind, one final delete quiesces the
    // key -- and then only the pinned reference may remain.
    fs.delete_object("b", "k").await.unwrap();
    assert_block_state(&shared, id, 1, fs.fs_root());
}

/// Clone-by-reference racing the DELETE of the record it clones (ADR 0014).
///
/// The clone takes its references under each block's stripe and writes its
/// record only after every one of them succeeded, so there are exactly two
/// outcomes and no third:
///
/// - the bump won the stripe: rc never reached zero, and the clone's record
///   names blocks that are still there;
/// - the delete's last decrement won: the clone finds nothing to reference,
///   refuses, gives back whatever it had taken, and writes no record.
///
/// The assertion is therefore not "the clone succeeded" -- it is that
/// success and refusal each imply their own state, checked per iteration.
/// A clone that could publish a record naming a freed block would show up
/// here as a resolve failure on a record that exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clone_by_reference_racing_delete_never_names_a_freed_block() {
    let dir = tempdir().unwrap();
    let (shared, namespaces) = store_with_namespaces(dir.path(), None, 1);
    let fs = namespaces[0].clone();
    fs.create_bucket("source").unwrap();
    fs.create_bucket("dest").unwrap();

    let data = b"cloned and deleted at the same time".repeat(120).to_vec();
    let id = shared.hasher().hash(&data);

    for _ in 0..STORM_ITERATIONS {
        put(&fs, "source", "k", data.clone()).await;

        let cloner = {
            let fs = fs.clone();
            tokio::spawn(async move {
                fs.clone_object_by_reference("source", b"k", "dest", b"k")
                    .await
                    .unwrap()
            })
        };
        let deleter = {
            let fs = fs.clone();
            tokio::spawn(async move { fs.delete_object("source", b"k").await.unwrap() })
        };
        let cloned = cloner.await.unwrap();
        deleter.await.unwrap();

        match cloned {
            Some(_) => {
                // The record exists, so every block it names must still be
                // there and readable -- that is the loss this test is for.
                let (_, paths) = fs
                    .get_object_paths("dest", b"k")
                    .unwrap()
                    .expect("a clone that landed has a record");
                for (path, _) in &paths {
                    assert!(
                        path.exists(),
                        "a landed clone must never name a freed block: {}",
                        path.display()
                    );
                }
            }
            None => {
                assert!(
                    fs.get_object_meta("dest", b"k").unwrap().is_none(),
                    "a refused clone must leave no record behind"
                );
            }
        }

        // Quiesce the round: both keys gone, and with them the block.
        fs.delete_object("source", b"k").await.unwrap();
        fs.delete_object("dest", b"k").await.unwrap();
        assert_block_state(&shared, id, 0, fs.fs_root());
    }
}

/// The reader race an overwrite inherits IS the delete race, unchanged.
///
/// A reader resolves its block list from the object record and then reads the
/// files. If the last reference to one of those blocks goes in between, the
/// file is unlinked underneath it -- and that is true whether the reference
/// went because the key was DELETED or because it was OVERWRITTEN. POSIX
/// keeps an already-open file alive through the unlink, so a reader holding
/// its handles reads through to completion; a reader that opens AFTER the
/// unlink fails loudly rather than being served anything.
///
/// Both legs do the same thing to the same object, one by overwriting the key
/// and one by deleting it, and assert the identical outcome. That is the
/// equivalence ADR 0008 rests on: the overwrite opens no new window, it
/// reaches the same release through the same primitive one commit later.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_overwrite_races_a_reader_exactly_as_a_delete_does() {
    for overwrite in [true, false] {
        let leg = if overwrite { "overwrite" } else { "delete" };
        let dir = tempdir().unwrap();
        let (shared, namespaces) = store_with_namespaces(dir.path(), None, 1);
        let fs = namespaces[0].clone();
        fs.create_bucket("b").unwrap();

        let data = format!("read me while it goes {leg} ")
            .repeat(64)
            .into_bytes();
        let id = shared.hasher().hash(&data);
        put(&fs, "b", "k", data.clone()).await;

        // The reader resolves its paths and OPENS them, then stops. The
        // record it read is about to stop being the visible one.
        let (_, paths) = fs.get_object_paths("b", "k").unwrap().expect("just PUT");
        let mut open: Vec<std::fs::File> = paths
            .iter()
            .map(|(path, _)| std::fs::File::open(path).expect("a live block file opens"))
            .collect();

        if overwrite {
            put(&fs, "b", "k", b"entirely other bytes".repeat(64).to_vec()).await;
        } else {
            fs.delete_object("b", "k").await.unwrap();
        }

        // Either way the old object's last reference is gone: record
        // removed, file unlinked.
        assert_block_state(&shared, id, 0, fs.fs_root());

        // The handles opened before it went still serve the old bytes, whole.
        let mut read_back = Vec::new();
        for file in &mut open {
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)
                .expect("an already-open handle survives the unlink");
            read_back.extend_from_slice(&buf);
        }
        assert_eq!(read_back, data, "{leg}: an open stream reads through");
        assert_eq!(
            shared.hasher().hash(&read_back),
            id,
            "{leg}: and reads back the block it opened"
        );

        // Opening after the unlink fails loudly. Never wrong bytes.
        for (path, _) in &paths {
            let err = std::fs::File::open(path).expect_err("open-after-unlink must fail");
            assert_eq!(
                err.kind(),
                std::io::ErrorKind::NotFound,
                "{leg}: the failure must say the file is gone"
            );
        }
    }
}
