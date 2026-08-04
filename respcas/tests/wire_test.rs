//! The wire itself: the two command forms, pipelining, frames that arrive in
//! pieces, keys and values that are not text, and what a client that sends
//! nonsense is told before it is hung up on.
//!
//! Everything here drives a socket directly. A client library would hide
//! exactly the things being asserted -- the reply SHAPES, the ORDER, and the
//! byte boundaries a frame is split at.

mod common;

use common::{TestServer, array, bulk, encode_command, error, integer, is_null, simple};

/// Both spellings of a command reach the same handler and get the same reply.
/// Inline is the form `valkey-cli --pipe` and every telnet-style client
/// speaks.
#[test]
fn an_inline_command_and_an_array_frame_are_one_protocol() {
    let server = TestServer::new();
    let mut conn = server.raw();

    conn.send(b"SET inline-key inline-value\r\n");
    assert_eq!(simple(&conn.reply()), "OK");

    conn.send_command(&[b"GET", b"inline-key"]);
    assert_eq!(bulk(&conn.reply()), b"inline-value");

    // And the other way around: written as a frame, read back inline.
    conn.send_command(&[b"SET", b"frame-key", b"frame-value"]);
    assert_eq!(simple(&conn.reply()), "OK");
    conn.send(b"GET frame-key\r\n");
    assert_eq!(bulk(&conn.reply()), b"frame-value");

    // Inline quoting is Redis's, so a value with a space in it survives.
    conn.send(b"SET quoted \"a b\\tc\"\r\n");
    assert_eq!(simple(&conn.reply()), "OK");
    conn.send_command(&[b"GET", b"quoted"]);
    assert_eq!(bulk(&conn.reply()), b"a b\tc");

    // A bare newline terminates an inline command as well as CRLF does.
    conn.send(b"PING\n");
    assert_eq!(simple(&conn.reply()), "PONG");
}

/// PING and ECHO answer the shapes clients match on: PING alone is a SIMPLE
/// string, PING with an argument and ECHO are BULK strings, and ECHO is
/// binary-safe -- which is what `valkey-cli --pipe` ends its stream with.
#[test]
fn ping_and_echo_answer_the_shapes_clients_match_on() {
    let server = TestServer::new();
    let mut conn = server.raw();

    conn.send_command(&[b"PING"]);
    assert_eq!(simple(&conn.reply()), "PONG");

    conn.send_command(&[b"PING", b"hello"]);
    assert_eq!(bulk(&conn.reply()), b"hello");

    let payload: &[u8] = &[0x00, 0xff, 0xfe, b'a', 0x80, b'\r', b'\n', 0x00];
    conn.send_command(&[b"ECHO", payload]);
    assert_eq!(bulk(&conn.reply()), payload);

    // The --pipe terminator in full: a bare CRLF (an empty inline command,
    // which carries no command and gets no reply) followed by the ECHO the
    // client waits for.
    conn.send(b"\r\n");
    conn.send_command(&[b"ECHO", b"twenty-random-bytes"]);
    assert_eq!(bulk(&conn.reply()), b"twenty-random-bytes");
}

/// Many commands in one write, one reply each, in order. The pipeline is what
/// a bulk loader does, and a reply that arrived out of order would be paired
/// with the wrong command by every client there is.
#[test]
fn a_pipeline_is_answered_in_order() {
    let server = TestServer::new();
    let mut conn = server.raw();

    let mut batch = Vec::new();
    for i in 0..50u32 {
        batch.extend_from_slice(&encode_command(&[
            b"SET",
            format!("key-{i:02}").as_bytes(),
            format!("value-{i}").as_bytes(),
        ]));
    }
    for i in 0..50u32 {
        batch.extend_from_slice(&encode_command(&[b"GET", format!("key-{i:02}").as_bytes()]));
    }
    batch.extend_from_slice(&encode_command(&[b"DBSIZE"]));
    conn.send(&batch);

    let replies = conn.replies(101);
    for reply in &replies[..50] {
        assert_eq!(simple(reply), "OK");
    }
    for (i, reply) in replies[50..100].iter().enumerate() {
        assert_eq!(
            bulk(reply),
            format!("value-{i}").as_bytes(),
            "reply {i} belongs to the command that asked for it"
        );
    }
    assert_eq!(integer(&replies[100]), 50);
}

/// A pipeline may change what the connection is looking at halfway through:
/// SELECT is answered in its place in the stream, and the commands behind it
/// land in the namespace it chose. A daemon that answered SELECT out of band
/// would write the second half of this batch into the first namespace.
#[test]
fn a_pipeline_spans_two_namespaces_through_select() {
    let server = TestServer::new();
    let mut setup = server.connect();
    let _: String = redis::cmd("NSNEW").arg("named").query(&mut setup).unwrap();
    let _: String = redis::cmd("NSNEW").arg("blobs").query(&mut setup).unwrap();
    let _: String = redis::cmd("NSSET")
        .arg("blobs")
        .arg("key_mode")
        .arg("cas")
        .query(&mut setup)
        .unwrap();

    let value = b"content addressed in the same breath".to_vec();
    let address = blake3::hash(&value).as_bytes().to_vec();

    let mut conn = server.raw();
    let mut batch = Vec::new();
    batch.extend_from_slice(&encode_command(&[b"SELECT", b"named"]));
    batch.extend_from_slice(&encode_command(&[b"SET", b"a-name", b"a value"]));
    batch.extend_from_slice(&encode_command(&[b"SELECT", b"blobs"]));
    batch.extend_from_slice(&encode_command(&[b"CSET", &value]));
    batch.extend_from_slice(&encode_command(&[b"GET", &address]));
    batch.extend_from_slice(&encode_command(&[b"GET", b"a-name"]));
    batch.extend_from_slice(&encode_command(&[b"SELECT", b"named"]));
    batch.extend_from_slice(&encode_command(&[b"GET", b"a-name"]));
    conn.send(&batch);

    let replies = conn.replies(8);
    assert_eq!(simple(&replies[0]), "OK");
    assert_eq!(simple(&replies[1]), "OK");
    assert_eq!(simple(&replies[2]), "OK");
    assert_eq!(
        bulk(&replies[3]),
        &address[..],
        "the content-addressed put answers with the key it computed"
    );
    assert_eq!(bulk(&replies[4]), &value[..]);
    assert!(
        is_null(&replies[5]),
        "the name lives in the other namespace, not this one"
    );
    assert_eq!(simple(&replies[6]), "OK");
    assert_eq!(bulk(&replies[7]), b"a value");
}

/// A command that arrives in pieces is answered when its last byte does, and
/// not before -- at every boundary a TCP segment can be cut on.
///
/// The PING in front of each fragment is the ordering point: its reply proves
/// the daemon has read that far, so the rest of the command is genuinely
/// arriving as a second read rather than being spliced into the first.
#[test]
fn a_command_split_at_any_boundary_still_answers_once() {
    let server = TestServer::new();
    let mut conn = server.raw();

    let value = vec![b'v'; 4096];
    let command = encode_command(&[b"SET", b"fragmented", &value]);

    // In order: inside the array header, inside a bulk length prefix, at the
    // end of the length prefix's CRLF, inside the value, and between the
    // value's trailing CR and LF.
    let cuts = [
        2,
        command.len() - value.len() - 4,
        command.len() - value.len() - 2,
        command.len() - value.len() / 2,
        command.len() - 1,
    ];

    for cut in cuts {
        let (head, tail) = command.split_at(cut);

        let mut first = b"PING\r\n".to_vec();
        first.extend_from_slice(head);
        conn.send(&first);
        assert_eq!(
            simple(&conn.reply()),
            "PONG",
            "the daemon answers what is complete and waits for what is not"
        );

        conn.send(tail);
        assert_eq!(simple(&conn.reply()), "OK", "the split at {cut} was healed");

        conn.send_command(&[b"GET", b"fragmented"]);
        assert_eq!(bulk(&conn.reply()), &value[..]);
        conn.send_command(&[b"DEL", b"fragmented"]);
        assert_eq!(integer(&conn.reply()), 1);
    }
}

/// A pipeline whose last command is incomplete: everything before it is
/// answered immediately, and the tail is answered when it arrives. A daemon
/// that waited for a whole buffer would stall a client that writes in
/// fixed-size chunks.
#[test]
fn a_partial_command_does_not_hold_up_the_ones_in_front_of_it() {
    let server = TestServer::new();
    let mut conn = server.raw();

    let complete = encode_command(&[b"SET", b"first", b"1"]);
    let trailing = encode_command(&[b"SET", b"second", b"2"]);
    let (head, tail) = trailing.split_at(trailing.len() - 3);

    let mut batch = complete.clone();
    batch.extend_from_slice(head);
    conn.send(&batch);
    assert_eq!(simple(&conn.reply()), "OK");

    conn.send(tail);
    assert_eq!(simple(&conn.reply()), "OK");

    conn.send_command(&[b"MGET", b"first", b"second"]);
    let reply = conn.reply();
    let values = array(&reply);
    assert_eq!(bulk(&values[0]), b"1");
    assert_eq!(bulk(&values[1]), b"2");
}

/// Keys and values are bytes. A key holding a CRLF is not a frame boundary
/// and a NUL is not a terminator -- which is what makes a 32-byte hash a
/// usable key (ADR 0014), and is worth pinning where the bytes are chosen to
/// look like protocol.
#[test]
fn keys_and_values_carry_crlf_and_nul_unharmed() {
    let server = TestServer::new();
    let mut conn = server.raw();

    let key: &[u8] = b"a\r\nkey\0with$3\r\nnasty*2\r\nbytes";
    let value: &[u8] = b"\r\n\0$5\r\nvalue\r\n*1\r\n\0\xff\xfe";

    conn.send_command(&[b"SET", key, value]);
    assert_eq!(simple(&conn.reply()), "OK");

    conn.send_command(&[b"GET", key]);
    assert_eq!(bulk(&conn.reply()), value);

    conn.send_command(&[b"EXISTS", key]);
    assert_eq!(integer(&conn.reply()), 1);

    conn.send_command(&[b"LENGTH", key]);
    assert_eq!(integer(&conn.reply()), value.len() as i64);

    // SCAN hands the key back exactly as it was stored.
    conn.send_command(&[b"SCAN", b"0"]);
    let reply = conn.reply();
    let page = array(&reply);
    let keys = array(&page[1]);
    assert_eq!(keys.len(), 1);
    assert_eq!(bulk(&keys[0]), key);

    conn.send_command(&[b"DEL", key]);
    assert_eq!(integer(&conn.reply()), 1);
    conn.send_command(&[b"EXISTS", key]);
    assert_eq!(integer(&conn.reply()), 0);
}

/// A client's mistake is answered and the connection stays open: an unknown
/// command, a wrong arity, and a frame that is not a command array are all
/// errors the next command survives.
#[test]
fn a_bad_command_is_an_error_the_connection_survives() {
    let server = TestServer::new();
    let mut conn = server.raw();

    conn.send_command(&[b"NOSUCHCOMMAND", b"x"]);
    assert!(
        error(&conn.reply()).contains("Unknown command"),
        "an unknown command names itself"
    );

    conn.send_command(&[b"GET"]);
    assert!(error(&conn.reply()).contains("Wrong number of arguments"));

    conn.send_command(&[b"ECHO", b"a", b"b"]);
    assert!(error(&conn.reply()).contains("Wrong number of arguments"));

    // A well-formed frame that is not an array is not a command.
    conn.send(b"$5\r\nhello\r\n");
    assert!(error(&conn.reply()).contains("must be an array"));

    // Still serving.
    conn.send_command(&[b"PING"]);
    assert_eq!(simple(&conn.reply()), "PONG");
}

/// Bytes that are not RESP are a different matter: a stream has no
/// resynchronisation point, so the daemon says so and hangs up rather than
/// reading on into a buffer it can never parse.
#[test]
fn a_protocol_error_is_answered_and_then_the_connection_is_closed() {
    let server = TestServer::new();

    // The inline form: a quote that never closes.
    let mut conn = server.raw();
    conn.send_command(&[b"SET", b"before", b"kept"]);
    assert_eq!(simple(&conn.reply()), "OK");
    conn.send(b"SET key \"unterminated\r\n");
    let reply = conn.reply();
    assert!(
        error(&reply).contains("Protocol error"),
        "the reply names the protocol: {}",
        error(&reply)
    );
    conn.expect_hangup();

    // The array form: a length that is not a number.
    let mut conn = server.raw();
    conn.send(b"*x\r\n");
    let reply = conn.reply();
    assert!(
        error(&reply).contains("Protocol error"),
        "the reply names the protocol: {}",
        error(&reply)
    );
    conn.expect_hangup();

    // The daemon itself is unharmed, and so is what it had already stored.
    let mut next = server.raw();
    next.send_command(&[b"GET", b"before"]);
    assert_eq!(bulk(&next.reply()), b"kept");
}

/// An inline command with no end is refused rather than buffered: a client
/// that opens one and never closes it would otherwise grow the daemon's read
/// buffer for as long as it kept sending.
#[test]
fn an_endless_inline_command_is_refused() {
    let server = TestServer::new();
    let mut conn = server.raw();

    conn.send(&vec![b'x'; 64 * 1024 + 1]);
    let reply = conn.reply();
    assert!(
        error(&reply).contains("too big inline request"),
        "{}",
        error(&reply)
    );
    conn.expect_hangup();
}
