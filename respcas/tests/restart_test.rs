//! What is still there after the daemon is not.
//!
//! Two ways of stopping are covered, because they fail differently. A clean
//! shutdown closes the store, so anything that survives it only proves the
//! store was written; a kill gives the process no chance to do anything, so
//! what survives it is what the ack had already made durable (ADR 0013). Both
//! run against `Durability::Fsync`, which is what the binary's own default
//! is -- the other tests in this crate leave the flush to the page cache
//! because their stores are thrown away.
//!
//! The rest is what a restart is allowed to find on disk: the store format
//! version, the block files, and a store laid out the way respcas laid them
//! out before ADR 0014.

mod common;

use common::{
    ChildServer, ServerConfig, TestServer, block_count, block_files, header_version, open_store,
    rewind_to_pre_0014_layout,
};
use redis::Connection;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

/// The address a client computes for itself.
fn address(value: &[u8]) -> Vec<u8> {
    blake3::hash(value).as_bytes().to_vec()
}

/// Every header sidecar anywhere under `root`, sorted -- so a test can say
/// which files exist rather than only that the ones it expected do.
fn sidecars_under(root: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, found: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, found);
            } else if entry.file_name() == "store_header.bin" {
                found.push(path);
            }
        }
    }

    let mut found = Vec::new();
    walk(root, &mut found);
    found.sort();
    found
}

/// A value that cannot fit in a record: several blocks of it.
fn big_value(tag: u8) -> Vec<u8> {
    const BLOCK: usize = 1 << 20;
    let mut data = Vec::with_capacity(2 * BLOCK + 4096);
    for chunk in 0..2u8 {
        data.extend(std::iter::repeat_n(tag ^ chunk, BLOCK));
    }
    data.extend(std::iter::repeat_n(tag, 4096));
    data
}

fn cas_namespace(conn: &mut Connection, name: &str) {
    let _: String = redis::cmd("NSNEW").arg(name).query(conn).unwrap();
    let _: String = redis::cmd("NSSET")
        .arg(name)
        .arg("key_mode")
        .arg("cas")
        .query(conn)
        .unwrap();
}

fn select(conn: &mut Connection, name: &str) {
    let _: String = redis::cmd("SELECT").arg(name).query(conn).unwrap();
}

/// Writes one of each kind of record and returns the cas key, so both halves
/// of a restart test say the same thing about what was written.
fn write_a_bit_of_everything(conn: &mut Connection, value: &[u8]) -> Vec<u8> {
    let _: String = redis::cmd("NSNEW").arg("named").query(conn).unwrap();
    select(conn, "named");
    let _: String = redis::cmd("SET")
        .arg("a-user-key")
        .arg("a user-keyed value")
        .query(conn)
        .expect("a user-keyed write must be acknowledged");

    cas_namespace(conn, "blobs");
    select(conn, "blobs");
    let key: Vec<u8> = redis::cmd("CSET")
        .arg(value)
        .query(conn)
        .expect("a content-addressed write must be acknowledged");
    assert_eq!(key, address(value));
    key
}

/// Reads back everything `write_a_bit_of_everything` wrote.
fn read_it_all_back(conn: &mut Connection, key: &[u8], value: &[u8]) {
    select(conn, "named");
    let stored: String = redis::cmd("GET")
        .arg("a-user-key")
        .query(conn)
        .expect("the user-keyed record must still be there");
    assert_eq!(stored, "a user-keyed value");

    select(conn, "blobs");
    let read: Vec<u8> = redis::cmd("GET")
        .arg(key)
        .query(conn)
        .expect("the block-backed record must still be there");
    assert!(read == value, "every block, still in order");
    let checked: i64 = redis::cmd("CHECK").arg(key).query(conn).unwrap();
    assert_eq!(checked, 1, "and it still hashes to its key");
}

/// A daemon that is asked to stop keeps everything it acknowledged.
#[test]
fn a_clean_shutdown_keeps_what_was_acknowledged() {
    let value = big_value(0x61);

    let mut server = TestServer::new_durable();
    let data_dir = server.data_dir().to_path_buf();
    let key = write_a_bit_of_everything(&mut server.connect(), &value);
    server.stop();

    let restarted = TestServer::reopen(&data_dir);
    read_it_all_back(&mut restarted.connect(), &key, &value);
}

/// A daemon that is killed outright keeps everything it acknowledged.
///
/// This one has to be the real binary in a real process: the in-process
/// server is a thread, and killing a thread takes the test with it. The
/// binary resolves its own defaults, which is where the fsync durability
/// comes from -- so an ack that survives this is an ack that was on disk
/// before it was sent (ADR 0013).
#[test]
fn killing_the_daemon_keeps_what_was_acknowledged() {
    let value = big_value(0x62);
    let dir = tempdir().expect("a temporary directory");
    let data_dir = dir.path().join("store");

    let server = ChildServer::spawn(&data_dir);
    let key = write_a_bit_of_everything(&mut server.connect(), &value);
    server.kill();

    let restarted = ChildServer::spawn(&data_dir);
    read_it_all_back(&mut restarted.connect(), &key, &value);
}

/// The store format version is raised exactly when the store first holds a
/// content-addressed namespace, and never lowered again (ADR 0014).
///
/// The raise is what buys an older build a clean refusal at the open, instead
/// of a msgpack failure somewhere in the middle of serving. So it must not
/// happen early -- a store that only ever holds user-keyed namespaces stays
/// openable by a pre-0014 build -- and it must not be undone when the mode is
/// switched back, because the store HAS held such a namespace.
#[test]
fn the_store_format_is_raised_when_the_first_cas_namespace_appears() {
    let mut server = TestServer::new();
    let data_dir = server.data_dir().to_path_buf();
    {
        let mut conn = server.connect();
        let _: String = redis::cmd("NSNEW").arg("named").query(&mut conn).unwrap();
        select(&mut conn, "named");
        for i in 0..4u8 {
            let _: String = redis::cmd("SET")
                .arg(format!("key-{i}"))
                .arg(vec![i; 4096])
                .query(&mut conn)
                .unwrap();
        }
    }
    server.stop();
    assert_eq!(
        header_version(&data_dir),
        3,
        "a store of user-keyed namespaces is a pre-0014 store"
    );

    // The first cas namespace raises it -- before the metadata an older build
    // cannot decode is written.
    let mut server = TestServer::reopen(&data_dir);
    {
        let mut conn = server.connect();
        cas_namespace(&mut conn, "blobs");
    }
    server.stop();
    assert_eq!(header_version(&data_dir), 4);

    // Switching the (still empty) namespace back does not lower it.
    let mut server = TestServer::reopen(&data_dir);
    {
        let mut conn = server.connect();
        let _: String = redis::cmd("NSSET")
            .arg("blobs")
            .arg("key_mode")
            .arg("userkey")
            .query(&mut conn)
            .expect("an empty namespace may go back");
    }
    server.stop();
    assert_eq!(
        header_version(&data_dir),
        4,
        "the store has held such a namespace; older builds keep refusing it"
    );

    // And this build opens it, which is the other half of the gate.
    let reopened = TestServer::reopen(&data_dir);
    let mut conn = reopened.connect();
    select(&mut conn, "named");
    let read: Vec<u8> = redis::cmd("GET").arg("key-0").query(&mut conn).unwrap();
    assert_eq!(read.len(), 4096);
}

/// No block file exists until a value needs one.
///
/// The `blocks/` directory itself appears at the first open of this build,
/// additively and unconditionally -- that is what makes a store from before
/// ADR 0014 able to serve a content-addressed namespace later. What must NOT
/// appear before it is needed is a block: a user-keyed namespace inlines
/// every value whatever its size, and giving it the block path would change
/// the durability and latency of writes nobody asked to change.
#[test]
fn no_block_file_exists_until_a_value_needs_one() {
    let mut server = TestServer::new();
    let data_dir = server.data_dir().to_path_buf();
    {
        let mut conn = server.connect();
        let _: String = redis::cmd("NSNEW").arg("named").query(&mut conn).unwrap();
        select(&mut conn, "named");
        // Well past a block's worth, in a namespace that has no block path.
        let _: String = redis::cmd("SET")
            .arg("big")
            .arg(vec![0x7eu8; 3 << 20])
            .query(&mut conn)
            .expect("a user-keyed namespace takes a value of any size");
    }
    server.stop();

    assert!(
        data_dir.join("blocks").is_dir(),
        "the block store is created at open, so an old store gains one"
    );
    assert_eq!(block_files(&data_dir), 0, "and nothing has been put in it");
    assert_eq!(block_count(&open_store(&data_dir)), 0);

    // The first content-addressed write is the one that needs blocks.
    let value = big_value(0x63);
    let mut server = TestServer::reopen(&data_dir);
    let key = {
        let mut conn = server.connect();
        cas_namespace(&mut conn, "blobs");
        select(&mut conn, "blobs");
        redis::cmd("CSET")
            .arg(&value)
            .query::<Vec<u8>>(&mut conn)
            .unwrap()
    };
    server.stop();

    let storage = open_store(&data_dir);
    let blocks = common::record(&storage, "blobs", &key)
        .expect("the record must be there")
        .blocks()
        .to_vec();
    assert!(blocks.len() > 1);
    assert_eq!(block_count(&storage), blocks.len());
    assert_eq!(block_files(&data_dir), blocks.len(), "one file per block");
}

/// A store laid out the way respcas laid one out before ADR 0014 -- fjall's
/// files straight in the data directory, no block store -- serves a
/// content-addressed namespace created on it, and keeps doing so across a
/// restart.
///
/// `layout_test` proves such a store OPENS. What is proved here is that it
/// works: the block store it never had appears beside it, a namespace is
/// switched to the cas mode on it, block-backed values are written and read,
/// and everything that was in it before is still in it.
///
/// The header sidecar is part of that: raising the store's version rewrites
/// the copy INSIDE the store, whichever of the two layouts the database is
/// in. It used to be derived as the database directory's parent, which on
/// this layout is the store's own parent -- so the raise wrote a
/// `store_header.bin` outside the store and left the copy an operator would
/// find stale at 3.
#[test]
fn a_store_from_before_the_layout_takes_a_cas_namespace_across_a_restart() {
    // The temporary directory IS the store: nothing may be written above it,
    // and this test would be writing into the system temp root if anything
    // were.
    let dir = tempdir().expect("a temporary directory");
    let data_dir = dir.path().to_path_buf();

    // Build a normal store with something in it, then rewind its layout.
    let mut server = TestServer::with(ServerConfig {
        data_dir: Some(data_dir.clone()),
        ..ServerConfig::default()
    });
    {
        let mut conn = server.connect();
        let _: String = redis::cmd("NSNEW").arg("legacy").query(&mut conn).unwrap();
        select(&mut conn, "legacy");
        let _: String = redis::cmd("SET")
            .arg("from-before")
            .arg("written by the old layout")
            .query(&mut conn)
            .unwrap();
    }
    server.stop();
    rewind_to_pre_0014_layout(&data_dir);
    assert_eq!(header_version(&data_dir), 3, "as an old store would be");

    // A daemon on it: the old namespace is there, and a cas one can be made.
    let value = big_value(0x64);
    let mut server = TestServer::with(ServerConfig {
        data_dir: Some(data_dir.clone()),
        ..ServerConfig::default()
    });
    let key = {
        let mut conn = server.connect();
        let stored: String = redis::cmd("NSINFO").arg("legacy").query(&mut conn).unwrap();
        assert!(stored.contains("name: legacy"));

        cas_namespace(&mut conn, "blobs");
        select(&mut conn, "blobs");
        let key: Vec<u8> = redis::cmd("CSET").arg(&value).query(&mut conn).unwrap();
        assert_eq!(key, address(&value));
        let read: Vec<u8> = redis::cmd("GET").arg(&key).query(&mut conn).unwrap();
        assert!(read == value);
        key
    };
    server.stop();

    // The block store appeared inside the old data directory, and nothing
    // moved.
    assert!(data_dir.join("version").is_file(), "still the old shape");
    assert!(!data_dir.join("db").exists(), "no migration happened");
    assert!(data_dir.join("blocks").join(".db").is_dir());
    assert!(block_files(&data_dir) > 1);

    // The sidecar the raise rewrote is the one inside the store, and it is
    // the only one: the store directory holds exactly one of them.
    assert_eq!(
        header_version(&data_dir),
        4,
        "the copy inside the store says what the store says"
    );
    let mut expected = vec![
        data_dir.join("store_header.bin"),
        data_dir.join("blocks").join("store_header.bin"),
    ];
    expected.sort();
    assert_eq!(
        sidecars_under(&data_dir),
        expected,
        "one sidecar for the store, one for the blocks database it gained"
    );

    // And a restart on the rewound store finds both namespaces intact.
    let restarted = TestServer::with(ServerConfig {
        data_dir: Some(data_dir.clone()),
        ..ServerConfig::default()
    });
    let mut conn = restarted.connect();
    select(&mut conn, "legacy");
    let old: String = redis::cmd("GET")
        .arg("from-before")
        .query(&mut conn)
        .unwrap();
    assert_eq!(old, "written by the old layout");

    select(&mut conn, "blobs");
    let read: Vec<u8> = redis::cmd("GET").arg(&key).query(&mut conn).unwrap();
    assert!(read == value, "the content-addressed record survived too");
    let checked: i64 = redis::cmd("CHECK").arg(&key).query(&mut conn).unwrap();
    assert_eq!(checked, 1);
}
