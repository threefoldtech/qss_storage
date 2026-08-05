use crate::common::TestServer;
use crate::helpers::*;
use redis::Connection;

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

/// RSCAN over hash keys: the mirror of SCAN.
///
/// `RSCAN 0` starts the walk at the LARGEST key -- `0` means "the beginning
/// of this walk", and a backward walk begins at the end of the tree, which is
/// what zdb's command does and what makes a reverse enumeration startable at
/// all. Handed a key instead, it resumes strictly below it, exclusive of the
/// cursor exactly as SCAN is exclusive of its own. Binary keys travel through
/// the cursor unharmed, which is the ADR 0014 part.
///
/// It used to answer an empty page to `RSCAN 0`: the cursor was filtered to
/// "no cursor" (`cmd.rs::parse_cursor`) and a backward walk with no cursor
/// ranged over everything below the EMPTY key, which is nothing
/// (`stores/fjall.rs::iter_kv_backward`). A client could not start a reverse
/// enumeration without already knowing the largest key, and even then never
/// saw that key.
#[test]
fn rscan_walks_backward_from_the_largest_key_or_from_the_cursor_it_is_given() {
    let server = TestServer::new();
    let mut conn = server.connect();
    select_cas(&mut conn, "blobs");

    let mut keys: Vec<Vec<u8>> = Vec::new();
    for i in 0..6u8 {
        let value = vec![i; 32 + i as usize];
        keys.push(redis::cmd("CSET").arg(&value).query(&mut conn).unwrap());
    }
    keys.sort();

    // The start cursor answers the whole namespace, largest key first.
    let (cursor, page): (Vec<u8>, Vec<Vec<u8>>) =
        redis::cmd("RSCAN").arg("0").query(&mut conn).unwrap();
    assert_eq!(cursor, b"0", "six keys, one page");
    let mut descending: Vec<Vec<u8>> = keys.clone();
    descending.reverse();
    assert_eq!(page, descending, "every key, largest first");

    // Handed the largest key, it walks the rest of the tree backward. The
    // cursor it was given is not in the answer.
    let (cursor, page): (Vec<u8>, Vec<Vec<u8>>) = redis::cmd("RSCAN")
        .arg(keys.last().unwrap())
        .query(&mut conn)
        .unwrap();
    assert_eq!(cursor, b"0");
    assert_eq!(
        page,
        descending[1..].to_vec(),
        "every other key, largest first"
    );

    // And the smallest key ends the walk: there is nothing below it.
    let (cursor, page): (Vec<u8>, Vec<Vec<u8>>) = redis::cmd("RSCAN")
        .arg(keys.first().unwrap())
        .query(&mut conn)
        .unwrap();
    assert_eq!(cursor, b"0");
    assert!(page.is_empty(), "the cursor is exclusive at the end too");
}

/// What a zdb-shaped client expects of `RSCAN 0`: the reverse of `SCAN 0`,
/// every key, largest first.
#[test]
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

/// Both walks, across page boundaries: every key exactly once, in opposite
/// orders, from a bare start cursor.
///
/// A page holds ten keys and there are twenty-five of them, so each direction
/// pages three times and hands back a cursor twice. That is where a cursor
/// that resumed inclusively (a key twice) or skipped one (a key never) shows
/// up, and it is the only place either can.
#[test]
fn scan_and_rscan_paginate_to_the_same_set_from_opposite_ends() {
    const KEYS: usize = 25;

    let server = TestServer::new();
    let mut conn = server.connect();
    select_cas(&mut conn, "blobs");

    let mut stored: Vec<Vec<u8>> = Vec::new();
    for i in 0..KEYS as u8 {
        let value = vec![i; 64];
        stored.push(redis::cmd("CSET").arg(&value).query(&mut conn).unwrap());
    }
    stored.sort();

    let walk = |conn: &mut Connection, command: &str| -> Vec<Vec<u8>> {
        let mut seen: Vec<Vec<u8>> = Vec::new();
        let mut cursor: Vec<u8> = b"0".to_vec();
        let mut pages = 0;
        loop {
            let (next, keys): (Vec<u8>, Vec<Vec<u8>>) = redis::cmd(command)
                .arg(&cursor)
                .query(conn)
                .unwrap_or_else(|e| panic!("{command} must answer: {e}"));
            pages += 1;
            assert!(pages <= KEYS, "{command} is not terminating");
            seen.extend(keys);
            if next == b"0" {
                break;
            }
            cursor = next;
        }
        assert_eq!(pages, 3, "{command}: ten to a page, twenty-five keys");
        seen
    };

    let forward = walk(&mut conn, "SCAN");
    assert_eq!(forward, stored, "SCAN: every key once, smallest first");

    let backward = walk(&mut conn, "RSCAN");
    let mut descending = stored.clone();
    descending.reverse();
    assert_eq!(backward, descending, "RSCAN: the same set, the other way");
}
