use super::*;
use crate::cas::GroupCommit;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

/// Long enough that every member of a test group has queued before
/// the deadline fires, short enough not to slow the suite down.
const WINDOW: Duration = Duration::from_millis(500);

/// A store whose station gathers for [`WINDOW`] before committing.
fn windowed_store(
    dir: &Path,
    cap: Option<usize>,
    ops: Option<Arc<dyn BlockDiskOps>>,
) -> (Arc<SharedBlockStore>, Arc<CasFS>) {
    let (shared, fs) =
        store_with_cap_and_station(dir, cap, ops, Some(GroupCommit { window: WINDOW }));
    (shared, Arc::new(fs))
}

async fn try_put(fs: &CasFS, key: &str, data: Vec<u8>) -> io::Result<Object> {
    let len = data.len();
    let stream = AsyncByteStream::new(futures::stream::once(async move { Ok(Bytes::from(data)) }));
    fs.store_single_object_and_meta(BUCKET, key, stream, len)
        .await
}

/// Everything a healthy store must NOT have on disk after a batch:
/// no temp residue, and no copy of a block at a depth its record does
/// not name.
fn assert_no_residue(shared: &SharedBlockStore, fs: &CasFS, id: &BlockId) {
    let block = shared
        .block_tree()
        .get_block(id.as_slice())
        .unwrap()
        .expect("the block must have a record");
    for depth in 1..=4u8 {
        if depth != block.depth() {
            assert!(
                !block_disk_path(id, depth, fs.fs_root().clone()).exists(),
                "a copy at depth {depth} is off-depth residue"
            );
        }
    }
    assert_eq!(
        std::fs::read_dir(fs.fs_root().join(".tmp"))
            .unwrap()
            .count(),
        0,
        "no temp residue"
    );
}

/// A lone request pays nothing for a station that is idle.
///
/// This is the ADR's first promise and the reason group commit can be
/// default-safe: natural batching merges only what was ALREADY
/// waiting behind an in-flight commit, so a request that finds the
/// committer idle commits immediately. At `group_commit_window = 0`,
/// the shipped default, there is no timer to wait on at all.
///
/// The contrast is the assertion that gives the first half its
/// meaning: the SAME lone PUT against a station with a one-second
/// window really does wait. Without that row, "fast" would prove
/// nothing about whether a timer exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lone_request_does_not_wait_at_window_zero() {
    let content = distinct_blocks("lonely", 1);

    let quick = tempdir().unwrap();
    let (shared, fs) = store_with_cap_and_station(
        quick.path(),
        None,
        None,
        Some(GroupCommit {
            window: Duration::ZERO,
        }),
    );
    let started = std::time::Instant::now();
    put(&fs, "alone", content.clone()).await;
    let idle = started.elapsed();

    assert!(
        idle < Duration::from_millis(500),
        "an idle committer must take a lone batch at once, took {idle:?}"
    );
    let stats = shared
        .group_commit_stats()
        .expect("the station must have run");
    assert_eq!(stats.groups, 1, "one group");
    assert_eq!(stats.members, 1, "of one member");
    assert_eq!(stats.mean_group_size(), Some(1.0));

    // The same request against a window, which is what makes the
    // measurement above mean something.
    let slow = tempdir().unwrap();
    let (_, fs) = store_with_cap_and_station(
        slow.path(),
        None,
        None,
        Some(GroupCommit {
            window: Duration::from_secs(1),
        }),
    );
    let started = std::time::Instant::now();
    put(&fs, "alone", content).await;
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(900),
        "a window is a real wait, or the row above proves nothing: {waited:?}"
    );
}

/// Two strangers in one group, both carrying the same new block X:
/// one insert, one bump, rc exactly 2, one file, no residue.
///
/// The ADR's cross-member question, pinned. Under ADR 0010 these two
/// requests would serialize on X's stripe and the second's in-tx
/// decision would see the first's COMMITTED record; here they are in
/// one transaction, and the second's `bump_block_rc` has to read the
/// first's UNCOMMITTED insert instead. Same answer, different
/// mechanism -- which is exactly why it needs its own test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_members_of_one_group_over_one_new_block_insert_once_and_bump_once() {
    const ROUNDS: usize = 10;

    let dir = tempdir().unwrap();
    let (shared, fs) = windowed_store(dir.path(), Some(64), None);

    for round in 0..ROUNDS {
        let shared_block = distinct_blocks(&format!("group-shared-{round}"), 1);
        let mut left = distinct_blocks(&format!("group-left-{round}"), 1);
        left.extend_from_slice(&shared_block);
        let mut right = distinct_blocks(&format!("group-right-{round}"), 1);
        right.extend_from_slice(&shared_block);
        let id = shared.hasher().hash(&shared_block);

        let a = {
            let fs = fs.clone();
            tokio::spawn(async move { put(&fs, &format!("a-{round}"), left).await })
        };
        let b = {
            let fs = fs.clone();
            tokio::spawn(async move { put(&fs, &format!("b-{round}"), right).await })
        };
        a.await.unwrap();
        b.await.unwrap();

        let block = shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .expect("round {round}: the shared block must have one record");
        assert_eq!(
            block.rc(),
            2,
            "round {round}: one insert plus one bump, across two members"
        );
        let path = block.disk_path(&id, fs.fs_root().clone());
        assert_eq!(
            shared.hasher().hash(&std::fs::read(&path).unwrap()),
            id,
            "round {round}: the surviving file is the block"
        );
        assert_no_residue(&shared, &fs, &id);
    }

    let stats = shared.group_commit_stats().unwrap();
    assert!(
        stats.largest >= 2,
        "the window must actually have merged strangers: {stats:?}"
    );
    assert_eq!(stats.degraded, 0, "nothing here should have failed");
}

/// The plant gate: counts stagings so a poison plant can wait for
/// every member's dedup pre-read, and holds the committer's renames
/// until the poison is in place. Ordering by cause, not by clock --
/// the 100ms sleep this replaced lost to a starved scheduler about
/// once in twenty loaded suite runs: the poisoned member's pre-read
/// slid past the plant, the PUT died before the station, and there
/// was no group left to degrade (126/900 replay scenarios under
/// 6-way starvation, every one on the pre-read path).
#[derive(Debug)]
struct PlantGateOps {
    real: RealDiskOps,
    staged: AtomicUsize,
    armed: AtomicBool,
    released: AtomicBool,
}

impl PlantGateOps {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            real: RealDiskOps,
            staged: AtomicUsize::new(0),
            armed: AtomicBool::new(false),
            released: AtomicBool::new(false),
        })
    }
}

impl BlockDiskOps for PlantGateOps {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.real.create_dir_all(path)
    }
    fn write_new_file(&self, path: &Path, contents: &[u8]) -> io::Result<()> {
        self.real.write_new_file(path, contents)?;
        self.staged.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn fsync_file(&self, path: &Path, data_only: bool) -> io::Result<()> {
        self.real.fsync_file(path, data_only)
    }
    fn fsync_dir(&self, path: &Path) -> io::Result<()> {
        self.real.fsync_dir(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        // The store-id marker rename at creation passes; only renames
        // after arming (the members' landings) wait for the plant.
        while self.armed.load(Ordering::SeqCst) && !self.released.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(1));
        }
        self.real.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.real.remove_file(path)
    }
    fn list_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        self.real.list_dir(path)
    }
    fn device_of(&self, path: &Path) -> io::Result<Option<u64>> {
        self.real.device_of(path)
    }
}

/// One poisoned member fails; every stranger in its group acks.
///
/// The poison is a block record that will not decode, planted while
/// the group is gathering -- so the member reaches the committer
/// healthy and dies inside the SHARED transaction, which is precisely
/// the case the ADR's degrade path exists for. What must happen: the
/// group transaction rolls back, every member is replayed on its own,
/// and only the member that named the corrupt record fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_poisoned_member_fails_alone_and_its_group_acks() {
    const STRANGERS: usize = 3;

    let dir = tempdir().unwrap();
    let gate = PlantGateOps::new();
    let (shared, fs) = windowed_store(dir.path(), Some(64), Some(gate.clone()));

    let poisoned_content = distinct_blocks("poisoned", 1);
    let poisoned_id = shared.hasher().hash(&poisoned_content);

    gate.armed.store(true, Ordering::SeqCst);
    let mut handles = Vec::new();
    handles.push({
        let fs = fs.clone();
        tokio::spawn(async move { try_put(&fs, "poisoned", poisoned_content).await })
    });
    for i in 0..STRANGERS {
        let fs = fs.clone();
        let content = distinct_blocks(&format!("innocent-{i}"), 1);
        handles.push(tokio::spawn(async move {
            try_put(&fs, &format!("innocent-{i}"), content).await
        }));
    }

    // The plant must land after every member's dedup pre-read and
    // before any group's transaction. Both edges are causal: a member
    // that has staged is past its pre-read, and a committer that
    // cannot rename cannot have reached its transaction. Garbage
    // where the poisoned member's block record belongs: its
    // `bump_block_rc` cannot decode it, which fails the transaction
    // the whole group shares.
    while gate.staged.load(Ordering::SeqCst) < STRANGERS + 1 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    shared
        .meta_store()
        .get_tree(crate::metastore::DEFAULT_BLOCK_TREE)
        .unwrap()
        .insert(poisoned_id.as_slice(), vec![0xffu8; 3])
        .unwrap();
    gate.released.store(true, Ordering::SeqCst);

    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.unwrap());
    }

    assert!(
        results[0].is_err(),
        "the member whose record will not decode must fail"
    );
    for (i, result) in results[1..].iter().enumerate() {
        assert!(
            result.is_ok(),
            "stranger {i} must ack anyway: {:?}",
            result.as_ref().err()
        );
    }

    let stats = shared.group_commit_stats().unwrap();
    assert_eq!(
        stats.degraded, 1,
        "exactly one group degraded to per-member replay: {stats:?}"
    );

    // And the strangers' objects are really there, not merely acked.
    for i in 0..STRANGERS {
        let obj = fs
            .get_object_meta(BUCKET, format!("innocent-{i}"))
            .unwrap()
            .expect("an acked object must have its record");
        for id in obj.blocks() {
            let block = shared
                .block_tree()
                .get_block(id.as_slice())
                .unwrap()
                .expect("and its blocks must be recorded");
            assert_eq!(block.rc(), 1);
            let path = block.disk_path(id, fs.fs_root().clone());
            assert_eq!(shared.hasher().hash(&std::fs::read(&path).unwrap()), *id);
        }
    }
    // The poisoned member acked nothing.
    assert!(
        fs.get_object_meta(BUCKET, "poisoned").unwrap().is_none(),
        "a request that failed must not have written an object record"
    );
}

/// Renames everything for real, then dies once, on the Nth one.
///
/// The kill fixture: a group whose files have all landed and whose
/// transaction never ran. Arming it for exactly one rename lets the
/// same store be used afterwards for the heal, which is the half of
/// residue class 1 that matters.
#[derive(Debug)]
struct DieAfterRenamesOps {
    real: RealDiskOps,
    die_on: usize,
    seen: AtomicUsize,
    armed: AtomicBool,
}

impl DieAfterRenamesOps {
    fn arm(die_on: usize) -> Arc<Self> {
        Arc::new(Self {
            real: RealDiskOps,
            die_on,
            seen: AtomicUsize::new(0),
            armed: AtomicBool::new(true),
        })
    }
}

impl BlockDiskOps for DieAfterRenamesOps {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.real.create_dir_all(path)
    }
    fn write_new_file(&self, path: &Path, contents: &[u8]) -> io::Result<()> {
        self.real.write_new_file(path, contents)
    }
    fn fsync_file(&self, path: &Path, data_only: bool) -> io::Result<()> {
        self.real.fsync_file(path, data_only)
    }
    fn fsync_dir(&self, path: &Path) -> io::Result<()> {
        self.real.fsync_dir(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.real.rename(from, to)?;
        let seen = self.seen.fetch_add(1, Ordering::SeqCst) + 1;
        assert!(
            !(seen >= self.die_on && self.armed.swap(false, Ordering::SeqCst)),
            "kill -9 between the group's last rename and its commit"
        );
        Ok(())
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.real.remove_file(path)
    }
    fn list_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        self.real.list_dir(path)
    }
    fn device_of(&self, path: &Path) -> io::Result<Option<u64>> {
        self.real.device_of(path)
    }
}

/// A kill at group width leaves residue class 1 and nothing else, and
/// a retry heals it in place.
///
/// The ADR's crash question: "up to `max_blocks_per_commit` orphan
/// block files -- the SAME bound and the SAME class as 0010, because
/// the group is capped by the same knob. The difference is
/// provenance, which fsck does not care about."
///
/// So the fixture kills the committer between the group's last rename
/// and its transaction, and the assertions are about the CLASS of
/// what is left: files at their final paths with no records (class
/// 1), no temp files, no off-depth copies (class 2), no record
/// without a file (which would be loss, not residue), and a bound of
/// one cap's worth.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kill_at_group_width_leaves_class_one_residue_that_a_retry_heals() {
    const MEMBERS: usize = 3;
    const BLOCKS_EACH: usize = 2;
    const CAP: usize = 64;

    let dir = tempdir().unwrap();
    let ops = DieAfterRenamesOps::arm(MEMBERS * BLOCKS_EACH);
    let (shared, fs) = windowed_store(dir.path(), Some(CAP), Some(ops.clone()));

    let contents: Vec<Vec<u8>> = (0..MEMBERS)
        .map(|i| distinct_blocks(&format!("killed-{i}"), BLOCKS_EACH))
        .collect();
    let ids: Vec<Vec<BlockId>> = contents
        .iter()
        .map(|content| {
            content
                .chunks(BLOCK_SIZE)
                .map(|chunk| shared.hasher().hash(chunk))
                .collect()
        })
        .collect();

    let mut handles = Vec::new();
    for (i, content) in contents.iter().enumerate() {
        let fs = fs.clone();
        let content = content.clone();
        handles.push(tokio::spawn(async move {
            try_put(&fs, &format!("killed-{i}"), content).await
        }));
    }
    for handle in handles {
        assert!(
            handle.await.unwrap().is_err(),
            "a group that died before its commit acks nobody"
        );
    }

    // Residue class 1: every file at its final path, not one record.
    let orphans: Vec<&BlockId> = ids.iter().flatten().collect();
    assert_eq!(orphans.len(), MEMBERS * BLOCKS_EACH);
    assert!(
        orphans.len() <= CAP,
        "the residue a single kill leaves is bounded by the cap"
    );
    assert_eq!(
        shared.block_tree().len().unwrap(),
        0,
        "not one record may exist: the transaction never ran"
    );
    let mut found = 0;
    for id in &orphans {
        let mut at_depth = 0;
        for depth in 1..=4u8 {
            if block_disk_path(id, depth, fs.fs_root().clone()).exists() {
                at_depth += 1;
                found += 1;
            }
        }
        assert!(
            at_depth <= 1,
            "an orphan must exist at ONE depth, never several (class 2 residue)"
        );
    }
    assert_eq!(
        found,
        orphans.len(),
        "every landed file is an orphan, and every orphan is a landed file"
    );
    assert_eq!(
        std::fs::read_dir(fs.fs_root().join(".tmp"))
            .unwrap()
            .count(),
        0,
        "no temp residue: the group renamed everything it staged"
    );

    // The heal: the same content, written again by a client that
    // retried. The orphans are adopted in place -- rename-over
    // installs identical bytes -- and the store comes out exact.
    for (i, content) in contents.iter().enumerate() {
        try_put(&fs, &format!("healed-{i}"), content.clone())
            .await
            .expect("the retry must succeed");
    }
    assert_eq!(
        shared.block_tree().len().unwrap(),
        MEMBERS * BLOCKS_EACH,
        "every orphan is now a recorded block"
    );
    for id in &orphans {
        let block = shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .expect("healed");
        assert_eq!(block.rc(), 1, "one holder: the retry");
        assert_no_residue(&shared, &fs, id);
        let path = block.disk_path(id, fs.fs_root().clone());
        assert_eq!(shared.hasher().hash(&std::fs::read(&path).unwrap()), **id);
    }
}

/// The workload the ADR is FOR: many small concurrent PUTs, all
/// acked, all readable, refcounts exact.
///
/// Deliberately at window zero -- the shipped default -- so this is
/// natural batching and nothing else. It does not assert a group size
/// (that would be asserting the scheduler); it asserts that whatever
/// grouping happened, the store is exactly what a store without a
/// station would hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flood_of_small_puts_through_the_station_is_exact() {
    const WRITERS: usize = 24;

    let dir = tempdir().unwrap();
    let (shared, fs) = store_with_cap_and_station(
        dir.path(),
        Some(64),
        None,
        Some(GroupCommit {
            window: Duration::ZERO,
        }),
    );
    let fs = Arc::new(fs);

    // Half the writers share one block, so the cross-member merge is
    // exercised by whatever groups the scheduler happens to form.
    let shared_block = b"a block every other writer carries".repeat(64).to_vec();
    let shared_id = shared.hasher().hash(&shared_block);

    let mut handles = Vec::new();
    for i in 0..WRITERS {
        let fs = fs.clone();
        let content = if i % 2 == 0 {
            shared_block.clone()
        } else {
            format!("small object {i} ").repeat(64).into_bytes()
        };
        handles.push(tokio::spawn(async move {
            put(&fs, &format!("small-{i}"), content).await
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    // Every object readable, every block exactly as recorded.
    for i in 0..WRITERS {
        let obj = fs
            .get_object_meta(BUCKET, format!("small-{i}"))
            .unwrap()
            .expect("every acked PUT must have its record");
        for id in obj.blocks() {
            let block = shared
                .block_tree()
                .get_block(id.as_slice())
                .unwrap()
                .expect("and every block of it must be recorded");
            let path = block.disk_path(id, fs.fs_root().clone());
            assert_eq!(shared.hasher().hash(&std::fs::read(&path).unwrap()), *id);
        }
    }

    // The contended block: one record, one file, one reference per
    // writer that named it -- however the groups fell.
    let block = shared
        .block_tree()
        .get_block(shared_id.as_slice())
        .unwrap()
        .expect("the shared block must have exactly one record");
    assert_eq!(
        block.rc(),
        WRITERS / 2,
        "one reference per holder, no more and no fewer"
    );
    assert_no_residue(&shared, &fs, &shared_id);

    let stats = shared.group_commit_stats().unwrap();
    assert_eq!(stats.degraded, 0, "nothing should have failed: {stats:?}");
    assert!(stats.groups > 0);

    // And the lifecycle still closes exactly: deleting every holder
    // takes the block with it.
    for i in (0..WRITERS).step_by(2) {
        fs.delete_object(BUCKET, &format!("small-{i}"))
            .await
            .unwrap();
    }
    assert!(
        shared
            .block_tree()
            .get_block(shared_id.as_slice())
            .unwrap()
            .is_none(),
        "as many references out as went in"
    );
}

/// The cap bounds a GROUP, not just a batch: members past it form the
/// next group rather than widening this one.
///
/// This is ADR 0011's review ask 2 made observable -- "the
/// transaction does not get bigger, its BOUND is unchanged". With a
/// cap of 2 and four one-block members gathered inside one window, no
/// group may carry more than two of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_never_carries_more_than_the_cap() {
    const MEMBERS: usize = 4;
    const CAP: usize = 2;

    let dir = tempdir().unwrap();
    let (shared, fs) = windowed_store(dir.path(), Some(CAP), None);

    let mut handles = Vec::new();
    for i in 0..MEMBERS {
        let fs = fs.clone();
        let content = distinct_blocks(&format!("capped-{i}"), 1);
        handles.push(tokio::spawn(async move {
            put(&fs, &format!("capped-{i}"), content).await
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    let stats = shared.group_commit_stats().unwrap();
    assert!(
        stats.largest <= CAP as u64,
        "a group carried more blocks than the cap: {stats:?}"
    );
    assert_eq!(
        stats.members, MEMBERS as u64,
        "every member was committed exactly once: {stats:?}"
    );
    assert!(
        stats.groups >= 2,
        "four members at a cap of two cannot be one group: {stats:?}"
    );
    assert_eq!(shared.block_tree().len().unwrap(), MEMBERS);
}
