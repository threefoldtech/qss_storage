//! What the binary does, seen from the outside: exit codes, the refusal to
//! create, the JSON contract, and `--repair` converging.
//!
//! Every store here is built through cas-storage's public API and closed
//! before the tool runs -- fjall holds a directory lock, which is exactly the
//! exclusivity fsck relies on. Residue is planted through public surfaces
//! only (`block_disk_path` and the filesystem); the crash fixtures are
//! crate-internal to cas-storage, so a dangling record is made the way a
//! Buffer-mode power cut makes one: PUT the object, then delete the file its
//! record names.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use cas_storage::metastore::block_disk_path;
use cas_storage::{AsyncByteStream, BlockId, CasFS, Durability, SharedMetrics, StorageEngine};
use tempfile::TempDir;

/// Exit codes, from the ADR's contract. Duplicated as literals on purpose:
/// these are what a shell script sees, so a test that imported the constants
/// could not catch them changing.
const CLEAN: i32 = 0;
const WARN: i32 = 1;
const CRITICAL: i32 = 2;
const COULD_NOT_RUN: i32 = 3;

/// Runs the tool against `dir` as both roots -- the layout the flags default
/// to, and the one that puts the store's own databases inside the blocks
/// root.
fn fsck(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_qss-storage-fsck"))
        .arg("--meta-root")
        .arg(dir)
        .arg("--fs-root")
        .arg(dir)
        // The tool searches ./qss_storage.toml when no --config is given; run
        // from a directory with no config so the test does not depend on the
        // checkout it runs in.
        .current_dir(dir)
        .args(args)
        .output()
        .expect("the fsck binary must run")
}

fn code(output: &Output) -> i32 {
    output
        .status
        .code()
        .expect("the tool must exit, not signal")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// Opens (creating, the first time) a store the way the daemon does.
///
/// Buffer durability because these stores are thrown away; the tool is told
/// the same, so it opens what the writer wrote.
fn open(dir: &Path) -> CasFS {
    CasFS::single_namespace(
        dir.to_path_buf(),
        dir.to_path_buf(),
        SharedMetrics::default(),
        StorageEngine::Fjall,
        Some(1),
        Some(Durability::Buffer),
        None,
        false,
        None,
        None,
    )
    .expect("the store must open")
}

/// Stores `data` under `bucket`/`key` and returns its single block id.
async fn put(fs: &CasFS, bucket: &str, key: &str, data: Vec<u8>) -> BlockId {
    let id = fs.hasher().hash(&data);
    let len = data.len();
    let stream = AsyncByteStream::new(futures::stream::once(async move {
        Ok(bytes::Bytes::from(data))
    }));
    fs.store_single_object_and_meta(bucket, key, stream, len)
        .await
        .expect("the object must store");
    id
}

/// The path the record for `id` names, via the pure path builder every reader
/// uses.
fn block_path(dir: &Path, id: &BlockId, depth: u8) -> PathBuf {
    block_disk_path(id, depth, dir.join("blocks"))
}

/// A store with two objects, closed. The baseline every residue test starts
/// from.
async fn healthy_store() -> TempDir {
    let dir = TempDir::new().unwrap();
    {
        let fs = open(dir.path());
        fs.create_bucket("photos").unwrap();
        put(&fs, "photos", "one", b"the first block".repeat(20).to_vec()).await;
        put(
            &fs,
            "photos",
            "two",
            b"the second block".repeat(20).to_vec(),
        )
        .await;
    }
    dir
}

/// A clean store says nothing and exits 0 -- including the store's own
/// databases, which the default one-root layout puts inside the blocks root.
#[tokio::test]
async fn a_clean_store_exits_zero() {
    let dir = healthy_store().await;
    let out = fsck(dir.path(), &[]);

    assert_eq!(code(&out), CLEAN, "{}", stdout(&out));
    assert!(stdout(&out).contains("no findings"), "{}", stdout(&out));
    assert!(
        dir.path().join("blocks").join(".db").is_dir(),
        "the tool must not have touched its own database"
    );
}

/// A path with no store is refused before anything is constructed. The
/// opposite behaviour -- creating one and reporting it clean -- is the
/// loudest possible wrong answer.
#[test]
fn a_path_with_no_store_exits_three() {
    let dir = TempDir::new().unwrap();
    let out = fsck(dir.path(), &[]);

    assert_eq!(code(&out), COULD_NOT_RUN);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("no store at"), "{stderr}");
    assert!(
        !dir.path().join("db").exists(),
        "refusing means creating nothing"
    );
}

/// Pins the exclusivity claim: a store another process still holds open is a
/// store fsck refuses to report on.
///
/// The claim under test is `docs/fsck.md` ("Exclusivity") and ADR 0005: fsck
/// builds no lock of its own and inherits fjall's LOCK file, so a daemon
/// holding the store shuts fsck out at the open. Every other test in this file
/// depends on that being true from the other side -- they all close their store
/// before running the tool -- and until now nothing checked it.
///
/// What was measured, rather than assumed: fjall really does refuse the second
/// open (`fjall::Error::Locked`). It is `std::fs::File::try_lock`, which is
/// `flock` on Linux, and flock associates the lock with the *open file
/// description* -- so a second open contends even from within one process. The
/// cross-process form is still the one pinned here, because that is the
/// deployment the docs describe and it also covers the exit-code contract the
/// same-process form cannot see.
///
/// That contract was not always true. The refusal used to arrive as a panic
/// (exit 101), because `FjallStore::new` `.unwrap()`ed the open; the store open
/// is fallible now, contention surfaces as `MetaError::StoreLocked`, and
/// `docs/fsck.md`'s "fails at the open (exit 3)" is what actually happens.
#[tokio::test]
async fn a_store_held_open_shuts_the_tool_out() {
    let dir = healthy_store().await;

    // Re-open and HOLD it, standing in for a running daemon.
    let held = open(dir.path());

    let out = fsck(dir.path(), &[]);
    let exit = code(&out);
    let stderr = String::from_utf8_lossy(&out.stderr);

    // Could-not-run, specifically: 0/1/2 all mean "fsck walked the store and
    // decided", which it must never do while someone else has it open, and
    // anything else (101) means it crashed rather than reported.
    assert_eq!(
        exit, COULD_NOT_RUN,
        "a locked store is could-not-run, not a verdict and not a crash: {stderr}"
    );

    // And it has to say so, in the operator's terms. The failure mode this
    // guards against is a message that blames something else -- a missing
    // store, a corrupt header -- and sends the operator after the wrong
    // problem.
    assert!(
        stderr.contains("locked by another process"),
        "the failure must name the lock contention, got: {stderr}"
    );
    assert!(
        !stderr.contains("panicked"),
        "a running daemon is routine, not a crash: {stderr}"
    );

    // Nothing on stdout: a report emitted here would be a report about a store
    // the tool never read.
    assert!(
        stdout(&out).is_empty(),
        "no report may be emitted: {}",
        stdout(&out)
    );

    // Once the holder lets go, the same store is fsck-able again -- so what
    // was pinned is the contention, not a store this test broke.
    drop(held);
    assert_eq!(code(&fsck(dir.path(), &[])), CLEAN);
}

/// A foreign file is an inconsistency, not loss: WARN, exit 1.
#[tokio::test]
async fn a_foreign_file_exits_one() {
    let dir = healthy_store().await;
    std::fs::write(dir.path().join("blocks").join("README"), b"not a block").unwrap();

    let out = fsck(dir.path(), &[]);

    assert_eq!(code(&out), WARN, "{}", stdout(&out));
    assert!(
        stdout(&out).contains("WARN foreign_file"),
        "{}",
        stdout(&out)
    );
}

/// A record whose file is gone is loss: CRITICAL, exit 2, and the report
/// names the object that is damaged.
#[tokio::test]
async fn a_dangling_record_exits_two() {
    let dir = TempDir::new().unwrap();
    let id = {
        let fs = open(dir.path());
        fs.create_bucket("photos").unwrap();
        let id = put(
            &fs,
            "photos",
            "damaged",
            b"bytes that vanish".repeat(20).to_vec(),
        )
        .await;
        // The Buffer-mode power cut: the record committed, the file's pages
        // did not survive.
        let record = fs.block_tree().unwrap().get_block(id.as_slice()).unwrap();
        let depth = record.expect("the record exists").depth();
        std::fs::remove_file(block_path(dir.path(), &id, depth)).unwrap();
        id
    };

    let out = fsck(dir.path(), &[]);

    assert_eq!(code(&out), CRITICAL, "{}", stdout(&out));
    let text = stdout(&out);
    assert!(text.contains("CRITICAL dangling_record"), "{text}");
    assert!(text.contains(&id.to_hex()), "{text}");
    assert!(text.contains("holder: object photos/damaged"), "{text}");
}

/// An orphan file is expected leakage: INFO, exit 0. It is still reported --
/// exit 0 means "nothing you must act on", not "nothing found".
#[tokio::test]
async fn an_orphan_file_is_reported_but_exits_zero() {
    let dir = healthy_store().await;
    let orphan = BlockId::from([0x5au8; 32]);
    let path = block_path(dir.path(), &orphan, 1);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"nobody's bytes").unwrap();

    let out = fsck(dir.path(), &[]);

    assert_eq!(code(&out), CLEAN, "{}", stdout(&out));
    assert!(
        stdout(&out).contains("INFO orphan_file"),
        "{}",
        stdout(&out)
    );
}

/// `--scrub` is the only optional pass: the same rotted store is silent
/// without it and CRITICAL with it.
#[tokio::test]
async fn scrub_is_what_finds_a_flipped_bit() {
    let dir = TempDir::new().unwrap();
    {
        let fs = open(dir.path());
        fs.create_bucket("photos").unwrap();
        let id = put(&fs, "photos", "rots", b"bytes that rot".repeat(20).to_vec()).await;
        let depth = fs
            .block_tree()
            .unwrap()
            .get_block(id.as_slice())
            .unwrap()
            .unwrap()
            .depth();
        let path = block_path(dir.path(), &id, depth);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 0x01;
        std::fs::write(&path, &bytes).unwrap();
    }

    let quiet = fsck(dir.path(), &[]);
    assert_eq!(code(&quiet), CLEAN, "{}", stdout(&quiet));

    let loud = fsck(dir.path(), &["--scrub"]);
    assert_eq!(code(&loud), CRITICAL, "{}", stdout(&loud));
    assert!(
        stdout(&loud).contains("CRITICAL corrupt_block"),
        "{}",
        stdout(&loud)
    );
}

/// The JSON is the scripting contract: it parses, and it carries the
/// documented top-level fields including the schema version.
#[tokio::test]
async fn json_parses_and_carries_the_schema_version() {
    let dir = healthy_store().await;
    std::fs::write(dir.path().join("blocks").join("README"), b"foreign").unwrap();

    let out = fsck(dir.path(), &["--json"]);
    assert_eq!(code(&out), WARN);

    let json: serde_json::Value = serde_json::from_str(&stdout(&out)).expect("--json must parse");
    assert_eq!(json["version"], 1);
    assert_eq!(json["exit_code"], 1);
    assert!(json["store"]["blocks_root"].is_string());
    assert_eq!(json["passes_run"][0], "recount");
    assert_eq!(json["summary"]["warn"], 1);
    assert_eq!(json["findings"][0]["class"], "foreign_file");
}

/// `--repair` converges: one run fixes what it found, and the next run over
/// the repaired store exits 0 with nothing to say.
#[tokio::test]
async fn repair_converges_and_the_next_run_is_clean() {
    let dir = TempDir::new().unwrap();
    let (live, orphan) = {
        let fs = open(dir.path());
        fs.create_bucket("photos").unwrap();
        let live = put(&fs, "photos", "kept", b"a live block".repeat(20).to_vec()).await;

        // An unreferenced file, a copy of a live block at a depth its record
        // does not name, and something that is not a block at all.
        let orphan = BlockId::from([0x77u8; 32]);
        let path = block_path(dir.path(), &orphan, 1);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"nobody's bytes").unwrap();

        let off_depth = block_path(dir.path(), &live, 4);
        std::fs::create_dir_all(off_depth.parent().unwrap()).unwrap();
        std::fs::write(&off_depth, b"a stale copy").unwrap();

        std::fs::write(dir.path().join("blocks").join("NOTES"), b"foreign").unwrap();
        (live, orphan)
    };

    // Before: the foreign file alone makes it WARN.
    let before = fsck(dir.path(), &[]);
    assert_eq!(code(&before), WARN, "{}", stdout(&before));

    let repaired = fsck(dir.path(), &["--repair"]);
    assert_eq!(code(&repaired), CLEAN, "{}", stdout(&repaired));
    let text = stdout(&repaired);
    assert!(text.contains("APPLIED delete_orphan"), "{text}");
    assert!(text.contains("APPLIED delete_off_depth"), "{text}");
    assert!(text.contains("APPLIED quarantine_foreign"), "{text}");

    // The residue is gone, the live block is not, and the foreign file was
    // set aside rather than deleted.
    assert!(!block_path(dir.path(), &orphan, 1).exists());
    assert!(!block_path(dir.path(), &live, 4).exists());
    assert!(
        dir.path()
            .join("blocks")
            .join(".quarantine")
            .join("NOTES")
            .is_file()
    );
    assert!(dir.path().join("blocks").join(".db").is_dir());

    // And again: nothing left to do.
    let again = fsck(dir.path(), &["--repair"]);
    assert_eq!(code(&again), CLEAN, "{}", stdout(&again));
    assert!(again_is_a_no_op(&stdout(&again)), "{}", stdout(&again));

    let plain = fsck(dir.path(), &[]);
    assert_eq!(code(&plain), CLEAN, "{}", stdout(&plain));
    assert!(stdout(&plain).contains("no findings"), "{}", stdout(&plain));
}

/// Whether a repair run reports having done nothing.
fn again_is_a_no_op(text: &str) -> bool {
    text.contains("nothing to repair") && text.contains("0 applied")
}

/// `qss-storage-fsck | head` must not panic. `println!` does, and a tool that
/// dies with a panic message at the end of a successful run reports the wrong
/// thing to whatever is reading its exit code.
///
/// The report here is deliberately larger than a pipe buffer (512 findings),
/// so closing the reader really does interrupt a write in progress rather
/// than a write that already fit.
#[tokio::test]
async fn a_closed_reader_is_not_a_panic() {
    use std::io::Read;
    use std::process::Stdio;

    let dir = healthy_store().await;
    for first in 0..16u8 {
        for second in 0..16u8 {
            let mut bytes = [0x5au8; 32];
            bytes[0] = first;
            bytes[1] = second;
            let id = BlockId::from(bytes);
            for depth in [1u8, 2] {
                let path = block_path(dir.path(), &id, depth);
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(&path, b"nobody's bytes").unwrap();
            }
        }
    }

    let mut child = Command::new(env!("CARGO_BIN_EXE_qss-storage-fsck"))
        .arg("--meta-root")
        .arg(dir.path())
        .arg("--fs-root")
        .arg(dir.path())
        .current_dir(dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // One byte proves the report started; dropping the handle then closes the
    // read end under the rest of it.
    let mut pipe = child.stdout.take().unwrap();
    let mut first = [0u8; 1];
    pipe.read_exact(&mut first).unwrap();
    drop(pipe);

    let out = child.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!stderr.contains("panicked"), "{stderr}");
    assert_eq!(
        out.status.code(),
        Some(CLEAN),
        "orphans are INFO, and a closed pipe does not change the verdict: {stderr}"
    );
}

/// The repair summary is JSON too, with its own schema version and the
/// post-repair report nested whole.
#[tokio::test]
async fn repair_json_carries_the_outcomes_and_the_post_report() {
    let dir = healthy_store().await;
    std::fs::write(dir.path().join("blocks").join("README"), b"foreign").unwrap();

    let out = fsck(dir.path(), &["--repair", "--json"]);
    assert_eq!(code(&out), CLEAN, "{}", stdout(&out));

    // Two documents on stdout: the report, then the summary. Take the second.
    let text = stdout(&out);
    let split = text.find("}\n{").expect("report then summary") + 2;
    let report: serde_json::Value = serde_json::from_str(&text[..split]).unwrap();
    let summary: serde_json::Value = serde_json::from_str(&text[split..]).unwrap();

    assert_eq!(
        report["exit_code"], 1,
        "the report was emitted BEFORE repair"
    );
    assert_eq!(summary["version"], 1);
    assert_eq!(summary["counts"]["applied"], 1);
    assert_eq!(summary["outcomes"][0]["action"], "quarantine_foreign");
    assert_eq!(summary["outcomes"][0]["status"], "applied");
    assert_eq!(summary["report"]["exit_code"], 0);
}
