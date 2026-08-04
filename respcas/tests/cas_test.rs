//! Content-addressed namespaces, from the wire (ADR 0014).

mod common;

use common::{
    RawConn, TestServer, block_count, block_files, bulk, error, integer, is_null, open_store,
    rc_of, record, simple,
};
use redis::Connection;

/// Creates `name` and switches it to the content-addressed key mode.
fn cas_namespace(conn: &mut Connection, name: &str) {
    let created: String = redis::cmd("NSNEW")
        .arg(name)
        .query(conn)
        .expect("the namespace must be created");
    assert_eq!(created, "OK");

    let switched: String = redis::cmd("NSSET")
        .arg(name)
        .arg("key_mode")
        .arg("cas")
        .query(conn)
        .expect("an empty namespace must accept the cas key mode");
    assert_eq!(switched, "OK");
}

fn nsinfo(conn: &mut Connection, name: &str) -> String {
    redis::cmd("NSINFO")
        .arg(name)
        .query(conn)
        .expect("NSINFO must answer")
}

/// Creates `name`, switches it to cas, and selects it on this connection.
fn select_cas(conn: &mut Connection, name: &str) {
    cas_namespace(conn, name);
    let _: String = redis::cmd("SELECT")
        .arg(name)
        .query(conn)
        .expect("the namespace must be selectable");
}

/// The address a client computes for itself, with stock tooling: BLAKE3-256
/// over the whole value.
fn address(value: &[u8]) -> Vec<u8> {
    blake3::hash(value).as_bytes().to_vec()
}

/// An address as the daemon prints it in an error message.
fn hex(key: &[u8]) -> String {
    key.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Creates `name`, switches it to cas, and selects it -- over a raw socket,
/// for the tests whose subject is the reply itself.
fn raw_select_cas(conn: &mut RawConn, name: &str) {
    conn.send_command(&[b"NSNEW", name.as_bytes()]);
    assert_eq!(simple(&conn.reply()), "OK");
    conn.send_command(&[b"NSSET", name.as_bytes(), b"key_mode", b"cas"]);
    assert_eq!(simple(&conn.reply()), "OK");
    conn.send_command(&[b"SELECT", name.as_bytes()]);
    assert_eq!(simple(&conn.reply()), "OK");
}

/// A value big enough to leave the inline threshold behind and take the
/// block write path: several 1 MiB blocks, distinct per tag.
fn big_value(tag: u8) -> Vec<u8> {
    const BLOCK: usize = 1 << 20;
    let mut data = Vec::with_capacity(2 * BLOCK + 4096);
    for chunk in 0..2u8 {
        data.extend(std::iter::repeat_n(tag ^ chunk, BLOCK));
    }
    data.extend(std::iter::repeat_n(tag, 4096));
    data
}

fn set(conn: &mut Connection, key: &[u8], value: &[u8]) -> redis::RedisResult<String> {
    redis::cmd("SET").arg(key).arg(value).query(conn)
}

fn get(conn: &mut Connection, key: &[u8]) -> Option<Vec<u8>> {
    redis::cmd("GET")
        .arg(key)
        .query(conn)
        .expect("GET must answer")
}

fn exists(conn: &mut Connection, key: &[u8]) -> bool {
    redis::cmd("EXISTS")
        .arg(key)
        .query(conn)
        .expect("EXISTS must answer")
}

/// The mode is selectable per namespace and reported by NSINFO, and it
/// survives in the store's metadata rather than in the connection.
#[test]
fn a_namespace_can_be_switched_to_the_cas_key_mode() {
    let server = TestServer::new();
    let mut conn = server.connect();

    cas_namespace(&mut conn, "blobs");
    assert!(nsinfo(&mut conn, "blobs").contains("mode: cas"));

    // The namespace next door is untouched: this is a per-namespace choice.
    assert!(nsinfo(&mut conn, "default").contains("mode: userkey"));

    // And a second connection sees the persisted mode, not a session flag.
    let mut other = server.connect();
    assert!(nsinfo(&mut other, "blobs").contains("mode: cas"));
}

/// The gate: a namespace that holds keys cannot change what its keys mean.
/// The refusal names the count, so an operator can see what is in the way.
#[test]
fn a_populated_namespace_refuses_the_key_mode_change() {
    let server = TestServer::new();
    let mut conn = server.connect();

    let _: String = redis::cmd("NSNEW")
        .arg("named")
        .query(&mut conn)
        .expect("the namespace must be created");
    let _: String = redis::cmd("SELECT")
        .arg("named")
        .query(&mut conn)
        .expect("the namespace must be selectable");
    let _: () = redis::cmd("SET")
        .arg("a-user-key")
        .arg("some value")
        .query(&mut conn)
        .expect("a userkey namespace stores what it is told");

    let refused: redis::RedisResult<String> = redis::cmd("NSSET")
        .arg("named")
        .arg("key_mode")
        .arg("cas")
        .query(&mut conn);
    let message = format!(
        "{}",
        refused.expect_err("a populated namespace must refuse")
    );
    assert!(message.contains("1 key"), "{message}");
    assert!(message.contains("empty"), "{message}");

    // Nothing moved: the namespace is still what it was.
    assert!(nsinfo(&mut conn, "named").contains("mode: userkey"));

    // Emptying it opens the gate again.
    let _: i64 = redis::cmd("DEL")
        .arg("a-user-key")
        .query(&mut conn)
        .expect("DEL must answer");
    let switched: String = redis::cmd("NSSET")
        .arg("named")
        .arg("key_mode")
        .arg("cas")
        .query(&mut conn)
        .expect("an emptied namespace accepts the change");
    assert_eq!(switched, "OK");
}

/// Only the modes that exist, and only the ones that are implemented.
#[test]
fn unknown_and_unimplemented_key_modes_are_refused() {
    let server = TestServer::new();
    let mut conn = server.connect();

    let _: String = redis::cmd("NSNEW")
        .arg("ns")
        .query(&mut conn)
        .expect("the namespace must be created");

    let err = redis::cmd("NSSET")
        .arg("ns")
        .arg("key_mode")
        .arg("sideways")
        .query::<String>(&mut conn)
        .expect_err("an unknown key mode must be refused");
    assert!(format!("{err}").contains("unknown key mode"), "{err}");

    let err = redis::cmd("NSSET")
        .arg("ns")
        .arg("key_mode")
        .arg("sequential")
        .query::<String>(&mut conn)
        .expect_err("the vestigial mode must be refused");
    assert!(format!("{err}").contains("not implemented"), "{err}");

    // A namespace can also go back, while it is empty.
    let _: String = redis::cmd("NSSET")
        .arg("ns")
        .arg("key_mode")
        .arg("cas")
        .query(&mut conn)
        .expect("cas is accepted");
    let _: String = redis::cmd("NSSET")
        .arg("ns")
        .arg("key_mode")
        .arg("userkey")
        .query(&mut conn)
        .expect("and an empty namespace can go back");
    assert!(nsinfo(&mut conn, "ns").contains("mode: userkey"));
}

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

/// A dedup hit in the SAME namespace is a pure acknowledgement: the record
/// does not move, and the bytes the client sent are discarded unread. The
/// visible consequence is the ADR's deliberate casualty -- wrong bytes under
/// a present address are answered +OK, and GET still returns the verified
/// content.
#[test]
fn a_dedup_hit_in_this_namespace_acks_and_discards_the_bytes() {
    let server = TestServer::new();
    let mut conn = server.connect();
    select_cas(&mut conn, "blobs");

    let value = b"stored once, sent twice".to_vec();
    let key = address(&value);
    assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");

    let before: i64 = redis::cmd("KEYTIME")
        .arg(&key)
        .query(&mut conn)
        .expect("KEYTIME must answer");

    // The same content again: idempotent success.
    assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");

    // Wrong bytes under an address that is already present: also +OK,
    // because nothing hashes data it is about to discard.
    assert_eq!(set(&mut conn, &key, b"not the content").unwrap(), "OK");

    assert_eq!(
        get(&mut conn, &key).as_deref(),
        Some(&value[..]),
        "the store cannot be poisoned: the record was never rewritten"
    );
    let after: i64 = redis::cmd("KEYTIME")
        .arg(&key)
        .query(&mut conn)
        .expect("KEYTIME must answer");
    assert_eq!(before, after, "nothing was written");
    let checked: i64 = redis::cmd("CHECK").arg(&key).query(&mut conn).unwrap();
    assert_eq!(checked, 1);
}

/// A hit in ANOTHER content-addressed namespace is served by reference: the
/// second namespace gets a record of its own naming the same blocks, and a
/// DELETE in one leaves the other readable.
#[test]
fn a_hit_in_another_cas_namespace_is_cloned_by_reference() {
    let server = TestServer::new();
    let mut conn = server.connect();

    cas_namespace(&mut conn, "first");
    cas_namespace(&mut conn, "second");

    let value = big_value(0x33);
    let key = address(&value);

    let _: String = redis::cmd("SELECT").arg("first").query(&mut conn).unwrap();
    assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");

    // The second namespace does not have it yet: EXISTS is namespace-scoped.
    let _: String = redis::cmd("SELECT").arg("second").query(&mut conn).unwrap();
    assert!(!exists(&mut conn, &key));

    // Storing it here takes a reference to the content that is already in
    // the store; the bytes on the wire are discarded.
    assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");
    assert_eq!(get(&mut conn, &key).as_deref(), Some(&value[..]));

    // Now delete it in the first namespace. The second still reads.
    let _: String = redis::cmd("SELECT").arg("first").query(&mut conn).unwrap();
    let removed: i64 = redis::cmd("DEL").arg(&key).query(&mut conn).unwrap();
    assert_eq!(removed, 1);
    assert!(!exists(&mut conn, &key));

    let _: String = redis::cmd("SELECT").arg("second").query(&mut conn).unwrap();
    assert_eq!(
        get(&mut conn, &key).as_deref(),
        Some(&value[..]),
        "each namespace holds its own reference: one DEL cannot strand the other"
    );
    let checked: i64 = redis::cmd("CHECK").arg(&key).query(&mut conn).unwrap();
    assert_eq!(checked, 1, "the clone's blocks are intact");

    // The last holder frees it.
    let removed: i64 = redis::cmd("DEL").arg(&key).query(&mut conn).unwrap();
    assert_eq!(removed, 1);
    assert!(get(&mut conn, &key).is_none());
}

/// A 32-byte key in a USER-KEYED namespace is a coincidence, not an address:
/// it is never a clone source, so a cas namespace asked for the same key
/// stores the bytes it was sent and keeps its own content.
#[test]
fn a_32_byte_user_key_is_never_a_clone_source() {
    let server = TestServer::new();
    let mut conn = server.connect();

    let _: String = redis::cmd("NSNEW").arg("named").query(&mut conn).unwrap();
    cas_namespace(&mut conn, "addressed");

    // A user-keyed namespace holding a 32-byte key whose value does NOT hash
    // to it -- which is entirely legal there.
    let value = b"the real content".to_vec();
    let key = address(&value);
    let _: String = redis::cmd("SELECT").arg("named").query(&mut conn).unwrap();
    assert_eq!(
        set(&mut conn, &key, b"something else entirely").unwrap(),
        "OK"
    );

    // The cas namespace must not take that as an address.
    let _: String = redis::cmd("SELECT")
        .arg("addressed")
        .query(&mut conn)
        .unwrap();
    assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");
    assert_eq!(
        get(&mut conn, &key).as_deref(),
        Some(&value[..]),
        "the value stored is the one that was verified, not the coincidence next door"
    );
    let checked: i64 = redis::cmd("CHECK").arg(&key).query(&mut conn).unwrap();
    assert_eq!(checked, 1);
}

/// cas + worm is the immutable archive the ADR promises: DEL is refused, so
/// an EXISTS answer can never be invalidated. A repeated SET of content that
/// is already there is still an acknowledgement -- it changes nothing, so
/// there is nothing for worm to protect.
#[test]
fn cas_plus_worm_refuses_deletes_and_still_acks_a_dedup_hit() {
    let server = TestServer::new();
    let mut conn = server.connect();
    select_cas(&mut conn, "archive");

    let value = b"kept forever".to_vec();
    let key = address(&value);
    assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");

    let _: String = redis::cmd("NSSET")
        .arg("archive")
        .arg("worm")
        .arg("1")
        .query(&mut conn)
        .expect("worm must be settable");

    let err = redis::cmd("DEL")
        .arg(&key)
        .query::<i64>(&mut conn)
        .expect_err("worm refuses deletes");
    assert!(format!("{err}").contains("worm"), "{err}");
    assert!(exists(&mut conn, &key), "the archive keeps what it has");

    // The probe-then-store workflow still works against a worm namespace.
    assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");

    // And new content can still be added: worm is write-once, not read-only.
    let other = b"added later".to_vec();
    let other_key = address(&other);
    assert_eq!(set(&mut conn, &other_key, &other).unwrap(), "OK");
    assert_eq!(get(&mut conn, &other_key).as_deref(), Some(&other[..]));
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

/// SCAN in a content-addressed namespace enumerates hash keys, and its
/// cursor survives the round trip -- a cursor that had been decoded as text
/// would resume somewhere else.
#[test]
fn scan_walks_hash_keys_and_its_cursor_round_trips() {
    let server = TestServer::new();
    let mut conn = server.connect();
    select_cas(&mut conn, "blobs");

    let mut stored: Vec<Vec<u8>> = Vec::new();
    for i in 0..25u8 {
        let value = vec![i; 64];
        let key: Vec<u8> = redis::cmd("CSET").arg(&value).query(&mut conn).unwrap();
        stored.push(key);
    }
    stored.sort();

    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut cursor: Vec<u8> = b"0".to_vec();
    loop {
        let (next, keys): (Vec<u8>, Vec<Vec<u8>>) = redis::cmd("SCAN")
            .arg(&cursor)
            .query(&mut conn)
            .expect("SCAN must answer");
        seen.extend(keys);
        if next == b"0" {
            break;
        }
        cursor = next;
    }

    assert_eq!(seen, stored, "every key exactly once, in tree order");
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

/// The reference counts a cross-namespace clone leaves behind, counted in the
/// store rather than inferred from the wire.
///
/// The wire says the clone is readable and that a DELETE next door does not
/// break it -- `a_hit_in_another_cas_namespace_is_cloned_by_reference` covers
/// that. What the wire cannot say is whether the count is EXACT, and an
/// off-by-one either way is a real fault: too low strands a live record when
/// the other namespace lets go, too high leaks the blocks forever. So the
/// daemon is stopped at every step and the numbers are read off the store.
#[test]
fn a_clone_holds_its_own_reference_and_the_counts_are_exact() {
    let value = big_value(0x2c);
    let key = address(&value);

    // Written once, in one namespace: one reference per block.
    let mut server = TestServer::new();
    let data_dir = server.data_dir().to_path_buf();
    {
        let mut conn = server.connect();
        cas_namespace(&mut conn, "first");
        cas_namespace(&mut conn, "second");
        let _: String = redis::cmd("SELECT").arg("first").query(&mut conn).unwrap();
        assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");
    }
    server.stop();

    let blocks = {
        let storage = open_store(&data_dir);
        let object = record(&storage, "first", &key).expect("the record must be there");
        assert!(!object.is_inlined(), "the value took the block path");
        let blocks = object.blocks().to_vec();
        assert!(blocks.len() > 1, "and needed more than one block");
        for block in &blocks {
            assert_eq!(rc_of(&storage, block), Some(1), "one holder, one reference");
        }
        assert_eq!(block_count(&storage), blocks.len());
        blocks
    };

    // The same address stored in the second namespace: no bytes move, and
    // every block gains exactly one reference.
    let mut running = TestServer::reopen(&data_dir);
    {
        let mut conn = running.connect();
        let _: String = redis::cmd("SELECT").arg("second").query(&mut conn).unwrap();
        assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");
    }
    running.stop();
    {
        let storage = open_store(&data_dir);
        for block in &blocks {
            assert_eq!(
                rc_of(&storage, block),
                Some(2),
                "each namespace accounts for itself"
            );
        }
        assert_eq!(
            block_count(&storage),
            blocks.len(),
            "the clone added a record, not a copy"
        );
        assert_eq!(block_files(&data_dir), blocks.len());
        let clone = record(&storage, "second", &key).expect("the clone must be there");
        assert_eq!(
            clone.blocks(),
            &blocks[..],
            "the clone names the same blocks, in the same order"
        );
    }

    // The source lets go: the count drops by one and nothing is freed.
    let mut running = TestServer::reopen(&data_dir);
    {
        let mut conn = running.connect();
        let _: String = redis::cmd("SELECT").arg("first").query(&mut conn).unwrap();
        let removed: i64 = redis::cmd("DEL").arg(&key).query(&mut conn).unwrap();
        assert_eq!(removed, 1);
    }
    running.stop();
    {
        let storage = open_store(&data_dir);
        assert!(record(&storage, "first", &key).is_none());
        for block in &blocks {
            assert_eq!(rc_of(&storage, block), Some(1), "the clone still holds one");
        }
        assert_eq!(block_files(&data_dir), blocks.len(), "no byte was freed");
    }

    // The last holder lets go: now everything goes.
    let mut running = TestServer::reopen(&data_dir);
    {
        let mut conn = running.connect();
        let _: String = redis::cmd("SELECT").arg("second").query(&mut conn).unwrap();
        assert_eq!(
            get(&mut conn, &key).as_deref(),
            Some(&value[..]),
            "still readable through the clone alone"
        );
        let removed: i64 = redis::cmd("DEL").arg(&key).query(&mut conn).unwrap();
        assert_eq!(removed, 1);
    }
    running.stop();
    {
        let storage = open_store(&data_dir);
        for block in &blocks {
            assert_eq!(rc_of(&storage, block), None, "the block record is gone");
        }
        assert_eq!(block_count(&storage), 0);
        assert_eq!(block_files(&data_dir), 0, "and so are the bytes");
    }
}

/// Everything that reads a namespace, reading one whose keys are hashes.
///
/// None of these commands was changed by ADR 0014, which is the point: a
/// content-addressed namespace is a namespace, and a key that happens to be
/// 32 binary bytes must not fall out of any of them.
#[test]
fn the_reading_commands_all_answer_over_hash_keys() {
    let server = TestServer::new();
    let mut conn = server.connect();
    select_cas(&mut conn, "blobs");

    // A handful of small values and one that needs blocks of its own, so the
    // answers cover both shapes of record.
    let mut stored: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for i in 0..8u8 {
        let value = vec![i; 100 + i as usize];
        let key: Vec<u8> = redis::cmd("CSET").arg(&value).query(&mut conn).unwrap();
        stored.push((key, value));
    }
    let big = big_value(0x0f);
    let big_key: Vec<u8> = redis::cmd("CSET").arg(&big).query(&mut conn).unwrap();
    stored.push((big_key.clone(), big.clone()));

    let size: i64 = redis::cmd("DBSIZE").query(&mut conn).unwrap();
    assert_eq!(size, stored.len() as i64, "DBSIZE counts hash keys");

    let mut expected: Vec<Vec<u8>> = stored.iter().map(|(key, _)| key.clone()).collect();
    expected.sort();

    let (cursor, forward): (Vec<u8>, Vec<Vec<u8>>) =
        redis::cmd("SCAN").arg("0").query(&mut conn).unwrap();
    assert_eq!(cursor, b"0", "nine keys fit in one page");
    assert_eq!(forward, expected, "SCAN walks the tree in key order");

    // LENGTH and KEYTIME read the record's envelope, which a block-backed
    // record has as much as an inline one does.
    for (key, value) in &stored {
        let length: u64 = redis::cmd("LENGTH").arg(key).query(&mut conn).unwrap();
        assert_eq!(length, value.len() as u64);

        let keytime: i64 = redis::cmd("KEYTIME").arg(key).query(&mut conn).unwrap();
        assert!(keytime > 1_700_000_000, "a real timestamp, not zero");
    }
    let missing: Option<u64> = redis::cmd("LENGTH")
        .arg(address(b"nothing stored under this"))
        .query(&mut conn)
        .unwrap();
    assert_eq!(missing, None, "LENGTH of a miss is nil");
    let missing: Option<i64> = redis::cmd("KEYTIME")
        .arg(address(b"nothing stored under this"))
        .query(&mut conn)
        .unwrap();
    assert_eq!(missing, None);

    // MGET assembles one reply out of records of both shapes, with a nil in
    // the middle for the address nobody wrote.
    let (first_key, first_value) = &stored[0];
    let values: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
        .arg(first_key)
        .arg(address(b"nothing stored under this"))
        .arg(&big_key)
        .query(&mut conn)
        .expect("MGET must answer");
    assert_eq!(values.len(), 3);
    assert_eq!(values[0].as_deref(), Some(&first_value[..]));
    assert_eq!(values[1], None);
    assert_eq!(values[2].as_deref(), Some(&big[..]));
}

/// RSCAN over hash keys, as it actually behaves.
///
/// It walks the tree backward from the cursor it is handed, exclusive of that
/// cursor -- the mirror of SCAN, which walks forward from its cursor and is
/// exclusive of it too. Binary keys travel through the cursor unharmed, which
/// is the ADR 0014 part and the part that could have broken.
///
/// KNOWN DEVIATION, deliberately pinned here rather than fixed: RSCAN has no
/// start cursor. `SCAN 0` means "from the beginning", but `RSCAN 0` does NOT
/// mean "from the end" -- `0` is filtered to "no cursor"
/// (`cmd.rs::parse_cursor`) and a backward walk with no cursor ranges over
/// everything below the EMPTY key, which is nothing
/// (`stores/fjall.rs::iter_kv_backward`). So a client cannot start a reverse
/// enumeration without already knowing the largest key, and even then the
/// largest key is never returned, because the cursor is exclusive. zdb, whose
/// command this is, walks from the end on `RSCAN 0`. The aspiration is the
/// ignored test below; changing it is a wire-visible change and not this
/// suite's to make.
#[test]
fn rscan_walks_backward_from_the_cursor_it_is_given() {
    let server = TestServer::new();
    let mut conn = server.connect();
    select_cas(&mut conn, "blobs");

    let mut keys: Vec<Vec<u8>> = Vec::new();
    for i in 0..6u8 {
        let value = vec![i; 32 + i as usize];
        keys.push(redis::cmd("CSET").arg(&value).query(&mut conn).unwrap());
    }
    keys.sort();

    // The start cursor answers an empty page, and says the walk is over.
    let (cursor, page): (Vec<u8>, Vec<Vec<u8>>) =
        redis::cmd("RSCAN").arg("0").query(&mut conn).unwrap();
    assert_eq!(cursor, b"0");
    assert!(
        page.is_empty(),
        "RSCAN has no start cursor: see the note above"
    );

    // Handed the largest key, it walks the rest of the tree backward. The
    // cursor it was given is not in the answer.
    let (cursor, page): (Vec<u8>, Vec<Vec<u8>>) = redis::cmd("RSCAN")
        .arg(keys.last().unwrap())
        .query(&mut conn)
        .unwrap();
    assert_eq!(cursor, b"0", "six keys, one page");
    let mut expected: Vec<Vec<u8>> = keys[..keys.len() - 1].to_vec();
    expected.reverse();
    assert_eq!(page, expected, "every other key, largest first");
}

/// What a zdb-shaped client expects of `RSCAN 0`: the reverse of `SCAN 0`,
/// every key, largest first.
///
/// Ignored because making it pass changes what an existing wire command
/// answers, which is not a change a test suite may make on its own. See the
/// note on `rscan_walks_backward_from_the_cursor_it_is_given`.
#[test]
#[ignore = "RSCAN has no start cursor; changing that is a wire-visible change"]
fn rscan_from_the_start_cursor_should_walk_from_the_largest_key() {
    let server = TestServer::new();
    let mut conn = server.connect();
    select_cas(&mut conn, "blobs");

    let mut keys: Vec<Vec<u8>> = Vec::new();
    for i in 0..6u8 {
        let value = vec![i; 32 + i as usize];
        keys.push(redis::cmd("CSET").arg(&value).query(&mut conn).unwrap());
    }
    keys.sort();
    keys.reverse();

    let (cursor, page): (Vec<u8>, Vec<Vec<u8>>) =
        redis::cmd("RSCAN").arg("0").query(&mut conn).unwrap();
    assert_eq!(cursor, b"0");
    assert_eq!(page, keys, "the reverse of SCAN, from a bare start cursor");
}
