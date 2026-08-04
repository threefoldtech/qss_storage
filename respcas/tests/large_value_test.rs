//! Values that are bigger than a guess.
//!
//! The RESP encoder used to size its output buffer at 512 bytes and retry at
//! 16 KiB, and reported anything larger as a write failure -- which drops the
//! connection with nothing sent, so a client asking for a 20 KiB value saw an
//! unexplained EOF. It was fixed in the ADR 0014 series, and this file is
//! what keeps it fixed: the sizes are chosen around that old ceiling, and
//! every one of them is asserted in BOTH kinds of namespace, because the bug
//! was in the wire and not in the storage.

mod common;

use common::{ChildServer, TestServer, bulk, error, integer, open_store, record, simple};
use redis::Connection;
use tempfile::tempdir;

/// The sizes that matter: just under the old 16 KiB ceiling, exactly on it,
/// just over it, a megabyte, and enough to need three 1 MiB blocks.
const SIZES: [usize; 5] = [
    (16 << 10) - 1,
    16 << 10,
    (16 << 10) + 1,
    1 << 20,
    (2 << 20) + 4113,
];

/// A value of `len` bytes that is not a run of one byte, so a truncated or
/// mis-assembled read cannot pass by looking the same.
fn payload(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

fn cas_namespace(conn: &mut Connection, name: &str) {
    let _: String = redis::cmd("NSNEW").arg(name).query(conn).unwrap();
    let _: String = redis::cmd("NSSET")
        .arg(name)
        .arg("key_mode")
        .arg("cas")
        .query(conn)
        .unwrap();
    let _: String = redis::cmd("SELECT").arg(name).query(conn).unwrap();
}

/// A user-keyed namespace serves a value of any size. Nothing about this
/// namespace changed in ADR 0014 -- which is exactly why the reply-size bug
/// was reachable here too, and why the regression is pinned here first.
#[test]
fn a_user_keyed_namespace_serves_a_value_of_any_size() {
    let server = TestServer::new();
    let mut conn = server.connect();

    for (n, size) in SIZES.iter().enumerate() {
        let key = format!("big-{size}");
        let value = payload(*size, n as u8);

        let _: String = redis::cmd("SET")
            .arg(&key)
            .arg(&value)
            .query(&mut conn)
            .unwrap_or_else(|e| panic!("a {size} byte SET must be accepted: {e}"));

        let read: Vec<u8> = redis::cmd("GET")
            .arg(&key)
            .query(&mut conn)
            .unwrap_or_else(|e| panic!("a {size} byte GET must answer: {e}"));
        assert_eq!(read.len(), *size, "the whole value comes back");
        assert!(read == value, "and it is the value that was stored");

        let length: u64 = redis::cmd("LENGTH").arg(&key).query(&mut conn).unwrap();
        assert_eq!(length, *size as u64);

        let checked: i64 = redis::cmd("CHECK").arg(&key).query(&mut conn).unwrap();
        assert_eq!(checked, 1, "the record still matches its checksum");
    }

    // Two large values in one reply: MGET's array is encoded whole, so its
    // size is the sum and then some.
    let values: Vec<Vec<u8>> = redis::cmd("MGET")
        .arg(format!("big-{}", SIZES[3]))
        .arg(format!("big-{}", SIZES[4]))
        .query(&mut conn)
        .expect("a multi-megabyte array reply is a reply");
    assert_eq!(values[0].len(), SIZES[3]);
    assert_eq!(values[1].len(), SIZES[4]);
}

/// The same sizes in a content-addressed namespace, where a value above the
/// inline threshold is not in its record at all: the reply is assembled from
/// blocks, and the size in front of it comes from the record.
#[test]
fn a_content_addressed_namespace_serves_a_value_of_any_size() {
    let server = TestServer::new();
    let mut conn = server.connect();
    cas_namespace(&mut conn, "blobs");

    for (n, size) in SIZES.iter().enumerate() {
        let value = payload(*size, 0x80 ^ n as u8);
        let expected = blake3::hash(&value).as_bytes().to_vec();

        let key: Vec<u8> = redis::cmd("CSET")
            .arg(&value)
            .query(&mut conn)
            .unwrap_or_else(|e| panic!("a {size} byte CSET must be accepted: {e}"));
        assert_eq!(key, expected, "the reply is the address of the value");

        let read: Vec<u8> = redis::cmd("GET")
            .arg(&key)
            .query(&mut conn)
            .unwrap_or_else(|e| panic!("a {size} byte GET must answer: {e}"));
        assert_eq!(read.len(), *size);
        assert!(read == value, "every block, in order");

        let length: u64 = redis::cmd("LENGTH").arg(&key).query(&mut conn).unwrap();
        assert_eq!(length, *size as u64);

        let checked: i64 = redis::cmd("CHECK").arg(&key).query(&mut conn).unwrap();
        assert_eq!(checked, 1, "the value still hashes to its key");
    }
}

/// The inline threshold is a boundary and behaves like one: at it a value is
/// its own record, one byte past it the record names blocks. Asserted on the
/// store rather than on the wire, because the wire deliberately cannot tell
/// the two apart.
#[test]
fn the_inline_threshold_decides_where_a_value_lives() {
    const CAP: usize = 4096;

    let mut server = TestServer::new_with_inline_threshold(CAP);
    let mut conn = server.connect();
    cas_namespace(&mut conn, "blobs");

    let mut keys = Vec::new();
    for size in [CAP - 1, CAP, CAP + 1] {
        let value = payload(size, size as u8);
        let key: Vec<u8> = redis::cmd("CSET").arg(&value).query(&mut conn).unwrap();

        let read: Vec<u8> = redis::cmd("GET").arg(&key).query(&mut conn).unwrap();
        assert!(read == value, "a {size} byte value reads back whole");
        let checked: i64 = redis::cmd("CHECK").arg(&key).query(&mut conn).unwrap();
        assert_eq!(checked, 1, "and CHECK re-hashes it against its key");

        keys.push((size, key));
    }

    // Now look at what was actually written. The daemon has to be stopped
    // first: fjall holds the store's directory lock.
    server.stop();
    let data_dir = server.data_dir().to_path_buf();
    let storage = open_store(&data_dir);

    for (size, key) in &keys {
        let object = record(&storage, "blobs", key).expect("the record must be there");
        assert_eq!(object.size(), *size as u64);
        if *size <= CAP {
            assert!(
                object.is_inlined(),
                "{size} bytes is at or under the threshold: the value is the record"
            );
            assert!(object.blocks().is_empty());
        } else {
            assert!(
                !object.is_inlined(),
                "{size} bytes is over the threshold: the record names blocks"
            );
            assert_eq!(object.blocks().len(), 1, "one block holds {size} bytes");
        }
    }

    assert_eq!(
        common::block_count(&storage),
        1,
        "exactly one value crossed the threshold"
    );
    assert_eq!(
        common::block_files(&data_dir),
        1,
        "and exactly one block file was written"
    );
}

/// The value cap is enforced against what a command DECLARES, before its
/// bytes are read. The proof is a command whose header is sent and whose body
/// never is: the refusal comes back anyway (ADR 0014).
#[test]
fn a_value_over_the_cap_is_refused_from_its_header_alone() {
    const CAP: usize = 64 * 1024;

    let server = TestServer::new_with_max_value_size(CAP);
    let mut conn = server.raw();

    // A SET whose value header claims one byte more than the cap, and whose
    // value is never sent.
    conn.send(format!("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n${}\r\n", CAP + 1).as_bytes());

    let reply = conn.reply();
    let message = error(&reply);
    assert!(message.contains(&(CAP + 1).to_string()), "{message}");
    assert!(message.contains("max_value_size"), "{message}");

    // And then the connection goes: the bytes that were refused are still on
    // their way, and nothing after them could be read as a command.
    conn.expect_hangup();

    // The daemon is unharmed and the cap is not a per-connection accident.
    let mut next = server.raw();
    next.send_command(&[b"SET", b"key", &vec![b'v'; CAP]]);
    assert_eq!(
        simple(&next.reply()),
        "OK",
        "a value at the cap is accepted"
    );
    next.send_command(&[b"LENGTH", b"key"]);
    assert_eq!(integer(&next.reply()), CAP as i64);
}

/// The cap is the connection's, not the namespace's: a user-keyed namespace
/// is bounded by the same knob as a content-addressed one, and the key is
/// counted too -- every bulk argument of the command is.
#[test]
fn the_cap_bounds_every_argument_of_every_namespace() {
    const CAP: usize = 8192;

    let server = TestServer::new_with_max_value_size(CAP);

    // A user-keyed namespace: the default one, untouched by ADR 0014.
    let mut conn = server.raw();
    conn.send_command(&[b"SET", b"under", &vec![b'v'; CAP]]);
    assert_eq!(simple(&conn.reply()), "OK");
    conn.send_command(&[b"SET", b"over", &vec![b'v'; CAP + 1]]);
    assert!(error(&conn.reply()).contains("over the"));
    conn.expect_hangup();

    // A key is a bulk argument like any other.
    let mut conn = server.raw();
    conn.send_command(&[b"GET", &vec![b'k'; CAP + 1]]);
    assert!(error(&conn.reply()).contains("max_value_size"));
    conn.expect_hangup();

    // Nothing over the cap was stored, and the one under it was.
    let mut next = server.raw();
    next.send_command(&[b"EXISTS", b"under"]);
    assert_eq!(integer(&next.reply()), 1);
    next.send_command(&[b"EXISTS", b"over"]);
    assert_eq!(integer(&next.reply()), 0);
    next.send_command(&[b"GET", b"under"]);
    assert_eq!(bulk(&next.reply()).len(), CAP);
}

/// The cap the operator writes in the config file is the cap the wire
/// enforces.
///
/// `max_value_size` has no flag, so the config file is the only way to set
/// it, and `resolve`'s own tests only prove the number comes out of the file
/// -- not that anything downstream is given it. This runs the real binary on
/// a real `qss_storage.toml` and asks the socket, which is the only place the
/// whole chain can be seen at once (ADR 0014).
#[test]
fn the_cap_in_the_config_file_is_the_cap_on_the_wire() {
    const CAP: usize = 4096;

    let dir = tempdir().expect("a temporary directory");
    let server = ChildServer::spawn_with_config(
        &dir.path().join("store"),
        &format!("[resp]\nmax_value_size = {CAP}\n"),
    );
    let mut conn = server.connect();

    let answer: String = redis::cmd("SET")
        .arg("under")
        .arg(vec![b'v'; CAP])
        .query(&mut conn)
        .expect("a value at the configured cap is accepted");
    assert_eq!(answer, "OK");

    let err = redis::cmd("SET")
        .arg("over")
        .arg(vec![b'v'; CAP + 1])
        .query::<String>(&mut conn)
        .expect_err("and one byte more is not");
    let message = format!("{err}");
    assert!(message.contains(&(CAP + 1).to_string()), "{message}");
    assert!(message.contains(&CAP.to_string()), "{message}");
    assert!(message.contains("max_value_size"), "{message}");

    // The daemon is unharmed, and the built-in 64 MiB default is nowhere in
    // sight -- the file's number is the one in force.
    let mut next = server.connect();
    let length: u64 = redis::cmd("LENGTH").arg("under").query(&mut next).unwrap();
    assert_eq!(length, CAP as u64);
    let present: i64 = redis::cmd("EXISTS").arg("over").query(&mut next).unwrap();
    assert_eq!(present, 0);
}
