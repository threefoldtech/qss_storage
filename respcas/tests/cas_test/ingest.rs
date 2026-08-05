use crate::common::{TestServer, bulk, error, integer, is_null, simple};
use crate::helpers::*;

/// Mode A, both spellings: the server hashes and answers with the key.
/// `SET "" v` and `CSET v` are the same operation and must agree byte for
/// byte, with the client's own BLAKE3 of the value.
#[test]
fn the_server_hashed_forms_agree_with_each_other_and_with_blake3() {
    let server = TestServer::new();
    let mut conn = server.connect();
    select_cas(&mut conn, "blobs");

    let value = b"the value the server is asked to address".to_vec();
    let expected = address(&value);

    let sentinel: Vec<u8> = redis::cmd("SET")
        .arg("")
        .arg(&value)
        .query(&mut conn)
        .expect("the empty-key form must answer with the key");
    let cset: Vec<u8> = redis::cmd("CSET")
        .arg(&value)
        .query(&mut conn)
        .expect("CSET must answer with the key");

    assert_eq!(sentinel, expected, "the reply is blake3 of the value");
    assert_eq!(cset, sentinel, "both spellings are one operation");
    assert_eq!(get(&mut conn, &expected).as_deref(), Some(&value[..]));
    assert!(exists(&mut conn, &expected));
}

/// Mode B: the client claims the address, and a value that does not hash to
/// it is refused with nothing written.
#[test]
fn a_client_hashed_put_is_verified_on_the_write_that_materializes_it() {
    let server = TestServer::new();
    let mut conn = server.connect();
    select_cas(&mut conn, "blobs");

    let value = b"content the client hashed itself".to_vec();
    let key = address(&value);

    // The dedup-upload workflow: probe, miss, transfer.
    assert!(!exists(&mut conn, &key));
    assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");
    assert!(exists(&mut conn, &key));
    assert_eq!(get(&mut conn, &key).as_deref(), Some(&value[..]));

    // A key that is not an address at all.
    let err = set(&mut conn, b"not-32-bytes", b"whatever").expect_err("the length is checked");
    assert!(format!("{err}").contains("32 bytes"), "{err}");

    // A lie about an address nothing has written yet: refused, nothing stored.
    let lie = address(b"some other content entirely");
    let err = set(&mut conn, &lie, b"these bytes hash to something else")
        .expect_err("a mismatch must be refused");
    assert!(format!("{err}").contains("does not hash"), "{err}");
    assert!(!exists(&mut conn, &lie), "a refused write stores nothing");
}

/// A value above the inline threshold rides the block write path, and reads
/// back whole. CHECK re-hashes it against its key -- the check that only a
/// content-addressed namespace can make about a record whose bytes are not
/// in the record.
#[test]
fn a_large_value_goes_through_the_block_path_and_reads_back_whole() {
    let server = TestServer::new();
    let mut conn = server.connect();
    select_cas(&mut conn, "blobs");

    let value = big_value(0x5a);
    let key: Vec<u8> = redis::cmd("CSET")
        .arg(&value)
        .query(&mut conn)
        .expect("a multi-block value stores");
    assert_eq!(key, address(&value));

    let read = get(&mut conn, &key).expect("a block-backed record reads back");
    assert_eq!(read.len(), value.len());
    assert_eq!(read, value, "every block, in order");

    let length: u64 = redis::cmd("LENGTH")
        .arg(&key)
        .query(&mut conn)
        .expect("LENGTH must answer");
    assert_eq!(length, value.len() as u64);

    let checked: i64 = redis::cmd("CHECK")
        .arg(&key)
        .query(&mut conn)
        .expect("CHECK must answer");
    assert_eq!(checked, 1, "the value still hashes to its key");

    // And the blocks really are on disk: the store is a meta+blocks pair now.
    assert!(
        server.data_dir().join("blocks").is_dir(),
        "the block store is where the tools look for it"
    );
}

/// Concurrent SETs of one key from several connections: the content is
/// identical by construction, so whatever the interleaving the key holds it
/// exactly once and reads back.
#[test]
fn concurrent_sets_of_one_key_converge() {
    use std::thread;

    let server = TestServer::new();
    let mut setup = server.connect();
    select_cas(&mut setup, "blobs");

    let value = big_value(0x77);
    let key = address(&value);

    let mut writers = Vec::new();
    for _ in 0..4 {
        let mut conn = server.connect();
        let value = value.clone();
        let key = key.clone();
        writers.push(thread::spawn(move || {
            let _: String = redis::cmd("SELECT").arg("blobs").query(&mut conn).unwrap();
            for _ in 0..3 {
                assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");
            }
        }));
    }
    for writer in writers {
        writer.join().expect("no writer may fail");
    }

    let size: i64 = redis::cmd("DBSIZE")
        .query(&mut setup)
        .expect("DBSIZE must answer");
    assert_eq!(size, 1, "one address, one record");
    assert_eq!(get(&mut setup, &key).as_deref(), Some(&value[..]));
    let checked: i64 = redis::cmd("CHECK").arg(&key).query(&mut setup).unwrap();
    assert_eq!(checked, 1);
}

/// The value cap: a command declaring more than the configured maximum is
/// refused, and it is refused from its header -- the daemon never buffers
/// the bytes it has already decided not to accept (ADR 0014).
#[test]
fn a_value_over_the_cap_is_refused() {
    let server = TestServer::new_with_max_value_size(4096);
    let mut conn = server.connect();
    select_cas(&mut conn, "blobs");

    // Under the cap: ordinary.
    let small = vec![0x11u8; 4096];
    let key: Vec<u8> = redis::cmd("CSET")
        .arg(&small)
        .query(&mut conn)
        .expect("a value at the cap is accepted");
    assert_eq!(key, address(&small));

    // Over it: refused. The connection goes with it, because the bytes that
    // were refused are still arriving and nothing after them can be parsed.
    let big = vec![0x22u8; 4097];
    let err = redis::cmd("CSET")
        .arg(&big)
        .query::<Vec<u8>>(&mut conn)
        .expect_err("a value over the cap must be refused");
    assert!(format!("{err}").contains("4097"), "{err}");
    assert!(format!("{err}").contains("max_value_size"), "{err}");

    // The store is unharmed, and the daemon is still serving.
    let mut next = server.connect();
    let _: String = redis::cmd("SELECT").arg("blobs").query(&mut next).unwrap();
    assert!(exists(&mut next, &key));
    assert!(!exists(&mut next, &address(&big)));
}

/// CSET is only a verb where a key is an address.
#[test]
fn cset_is_refused_outside_a_cas_namespace() {
    let server = TestServer::new();
    let mut conn = server.connect();

    let err = redis::cmd("CSET")
        .arg(b"anything")
        .query::<Vec<u8>>(&mut conn)
        .expect_err("the default namespace is user-keyed");
    assert!(
        format!("{err}").contains("content-addressed namespace"),
        "{err}"
    );
}

/// The reply SHAPES, read off the socket rather than through a client library
/// that would coerce them into whatever the test asked for.
///
/// A client tells the two ingest modes apart by what comes back: mode A
/// answers with the key it computed, as a bulk string of exactly 32 bytes,
/// and mode B -- which already has the key -- answers `+OK`. Both of those
/// are wire contract (ADR 0014), and so is the text of the refusal a client
/// gets when it lies about an address.
#[test]
fn the_content_addressed_replies_have_the_shapes_a_client_matches_on() {
    let server = TestServer::new();
    let mut conn = server.raw();
    raw_select_cas(&mut conn, "blobs");

    // Mode A, both spellings: a bulk string, and exactly 32 bytes of it. A
    // 64-character hex bulk would satisfy a test that only compared strings.
    let value = b"the reply is the address".to_vec();
    conn.send_command(&[b"SET", b"", &value]);
    let reply = conn.reply();
    assert_eq!(bulk(&reply).len(), 32, "an address is 32 raw bytes");
    assert_eq!(bulk(&reply), address(&value));

    let other = b"and this one too".to_vec();
    conn.send_command(&[b"CSET", &other]);
    let reply = conn.reply();
    assert_eq!(bulk(&reply).len(), 32);
    assert_eq!(bulk(&reply), address(&other));

    // Mode B: the client named the address, so it gets the same +OK a
    // user-keyed SET has always answered with -- a SIMPLE string, not a bulk.
    let claimed = b"claimed by the client".to_vec();
    conn.send_command(&[b"SET", &address(&claimed), &claimed]);
    assert_eq!(simple(&conn.reply()), "OK");

    // The verify-reject error in full: the prefix clients match on, and the
    // two addresses an operator needs to tell which side was wrong.
    let lie = address(b"one thing");
    conn.send_command(&[b"SET", &lie, b"another thing"]);
    let reply = conn.reply();
    let message = error(&reply).to_string();
    assert!(message.starts_with("ERR "), "{message}");
    assert!(
        message.contains("does not hash to the key it was sent under"),
        "{message}"
    );
    assert!(
        message.contains(&hex(&address(b"another thing"))),
        "the message names what the value actually hashes to: {message}"
    );
    assert!(
        message.contains(&hex(&lie)),
        "and the address that was claimed: {message}"
    );

    // The reads a probe-then-upload client makes, in their answering shapes.
    conn.send_command(&[b"EXISTS", &address(&value)]);
    assert_eq!(integer(&conn.reply()), 1);
    conn.send_command(&[b"EXISTS", &lie]);
    assert_eq!(integer(&conn.reply()), 0, "a refused write left nothing");
    conn.send_command(&[b"GET", &address(&value)]);
    assert_eq!(bulk(&conn.reply()), &value[..]);
    conn.send_command(&[b"GET", &lie]);
    assert!(is_null(&conn.reply()), "a miss is nil, not an empty bulk");
}

/// A key in a content-addressed namespace is 32 bytes or it is nothing.
///
/// Empty is the one exception, and it is not a short key: it is the sentinel
/// that means "compute the address for me". Everything between and beyond is
/// refused with the same message, and refused before anything is stored.
#[test]
fn a_key_that_is_not_an_address_is_refused_at_every_other_length() {
    let server = TestServer::new();
    let mut conn = server.raw();
    raw_select_cas(&mut conn, "blobs");

    let value = b"a value that will not be stored".to_vec();
    for length in [1usize, 31, 33, 64] {
        let key = vec![0xabu8; length];
        conn.send_command(&[b"SET", &key, &value]);
        let reply = conn.reply();
        let message = error(&reply).to_string();
        assert!(
            message.contains("32 bytes"),
            "a {length} byte key must be refused by length: {message}"
        );
        conn.send_command(&[b"DBSIZE"]);
        assert_eq!(integer(&conn.reply()), 0, "and store nothing on the way");
    }

    // Zero bytes is the sentinel, not a short key.
    conn.send_command(&[b"SET", b"", &value]);
    assert_eq!(bulk(&conn.reply()), address(&value));
    conn.send_command(&[b"DBSIZE"]);
    assert_eq!(integer(&conn.reply()), 1);

    // Reads are not length-checked: a key that cannot be an address is simply
    // a key nothing is stored under, which is what the probe workflow needs
    // (a client asking about garbage gets an answer, not a hangup).
    conn.send_command(&[b"GET", &[0xabu8; 31]]);
    assert!(is_null(&conn.reply()));
    conn.send_command(&[b"EXISTS", &[0xabu8; 33]]);
    assert_eq!(integer(&conn.reply()), 0);
    conn.send_command(&[b"DEL", &[0xabu8; 31]]);
    assert_eq!(integer(&conn.reply()), 0);
}
