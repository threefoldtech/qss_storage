use crate::common::TestServer;
use crate::helpers::*;

/// What NSINFO says a namespace holds: the logical bytes of its records,
/// moving with every write and delete.
///
/// Logical, not disk -- the sum of what clients stored. It is the number a
/// `max_size` quota is spent in, so it has to be right about overwrites (the
/// difference, not the addition) and about deletes.
#[test]
fn nsinfo_reports_the_logical_bytes_the_namespace_holds() {
    let server = TestServer::new();
    let mut conn = server.connect();

    nsnew(&mut conn, "counted");
    assert_eq!(
        field(&nsinfo(&mut conn, "counted"), "data_size_bytes"),
        "0",
        "a fresh namespace holds nothing"
    );

    select(&mut conn, "counted");
    let _: String = redis::cmd("SET")
        .arg("a")
        .arg(vec![1u8; 100])
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("SET")
        .arg("b")
        .arg(vec![2u8; 250])
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        field(&nsinfo(&mut conn, "counted"), "data_size_bytes"),
        "350"
    );

    // An overwrite applies the difference.
    let _: String = redis::cmd("SET")
        .arg("a")
        .arg(vec![3u8; 10])
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        field(&nsinfo(&mut conn, "counted"), "data_size_bytes"),
        "260"
    );

    // A delete gives the bytes back; deleting nothing gives nothing back.
    let removed: i64 = redis::cmd("DEL").arg("b").query(&mut conn).unwrap();
    assert_eq!(removed, 1);
    assert_eq!(
        field(&nsinfo(&mut conn, "counted"), "data_size_bytes"),
        "10"
    );
    let removed: i64 = redis::cmd("DEL").arg("b").query(&mut conn).unwrap();
    assert_eq!(removed, 0);
    assert_eq!(
        field(&nsinfo(&mut conn, "counted"), "data_size_bytes"),
        "10"
    );

    let _: i64 = redis::cmd("DEL").arg("a").query(&mut conn).unwrap();
    assert_eq!(field(&nsinfo(&mut conn, "counted"), "data_size_bytes"), "0");

    // Every namespace counts only itself.
    nsnew(&mut conn, "next-door");
    assert_eq!(
        field(&nsinfo(&mut conn, "next-door"), "data_size_bytes"),
        "0"
    );
}

/// A content-addressed namespace counts a dedup hit once, and a clone in
/// full (ADR 0014).
///
/// A second SET of content the namespace already has stores nothing, so it
/// adds nothing. A hit in ANOTHER namespace stores no bytes either -- the
/// blocks are referenced, not copied -- but it is a record of that size in
/// this namespace, and this namespace can be the holder that keeps those
/// bytes alive. So the ledger charges it in full: a clone is a store.
#[test]
fn a_dedup_hit_counts_once_and_a_clone_counts_in_full() {
    let server = TestServer::new();
    let mut conn = server.connect();

    for name in ["first", "second"] {
        nsnew(&mut conn, name);
        set_property(&mut conn, name, "key_mode", "cas");
    }

    let value = vec![0x5au8; 4096];
    select(&mut conn, "first");
    let key: Vec<u8> = redis::cmd("CSET").arg(&value).query(&mut conn).unwrap();
    assert_eq!(
        field(&nsinfo(&mut conn, "first"), "data_size_bytes"),
        "4096"
    );

    // The same content again: acknowledged, stored once, counted once.
    let same: Vec<u8> = redis::cmd("CSET").arg(&value).query(&mut conn).unwrap();
    assert_eq!(same, key);
    assert_eq!(
        field(&nsinfo(&mut conn, "first"), "data_size_bytes"),
        "4096"
    );

    // The other namespace takes a reference to it, and is charged for it.
    select(&mut conn, "second");
    let _: String = redis::cmd("SET")
        .arg(&key)
        .arg(&value)
        .query(&mut conn)
        .expect("the content is in the store, so this is a clone");
    assert_eq!(
        field(&nsinfo(&mut conn, "second"), "data_size_bytes"),
        "4096",
        "a clone is a store on the ledger"
    );
    assert_eq!(
        field(&nsinfo(&mut conn, "first"), "data_size_bytes"),
        "4096",
        "and the namespace it was cloned from is unmoved"
    );

    // One of them letting go leaves the other's charge exactly where it is.
    let removed: i64 = redis::cmd("DEL").arg(&key).query(&mut conn).unwrap();
    assert_eq!(removed, 1);
    assert_eq!(field(&nsinfo(&mut conn, "second"), "data_size_bytes"), "0");
    assert_eq!(
        field(&nsinfo(&mut conn, "first"), "data_size_bytes"),
        "4096"
    );
}

/// FLUSH empties a namespace, and the ledger says so.
///
/// The one path that removes records without deleting them one by one: the
/// namespace is dropped and created back, so its counter goes with it rather
/// than being decremented per record.
#[test]
fn flushing_a_namespace_empties_its_ledger_too() {
    let server = TestServer::new();
    let mut conn = server.connect();

    nsnew(&mut conn, "flushable");
    set_property(&mut conn, "flushable", "password", "s3cret");
    set_property(&mut conn, "flushable", "public", "0");
    assert_eq!(select_with(&mut conn, "flushable", "s3cret"), "OK");

    for i in 0..4u8 {
        let _: String = redis::cmd("SET")
            .arg(format!("chunk-{i}"))
            .arg(vec![i; 1024])
            .query(&mut conn)
            .unwrap();
    }
    assert_eq!(
        field(&nsinfo(&mut conn, "flushable"), "data_size_bytes"),
        "4096"
    );

    let flushed: String = redis::cmd("FLUSH")
        .query(&mut conn)
        .expect("a private password-protected namespace may be flushed");
    assert_eq!(flushed, "OK");

    let size: i64 = redis::cmd("DBSIZE").query(&mut conn).unwrap();
    assert_eq!(size, 0);
    assert_eq!(
        field(&nsinfo(&mut conn, "flushable"), "data_size_bytes"),
        "0",
        "an emptied namespace holds no bytes"
    );

    // And the emptied namespace counts from zero again.
    let _: String = redis::cmd("SET")
        .arg("after")
        .arg(vec![7u8; 32])
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        field(&nsinfo(&mut conn, "flushable"), "data_size_bytes"),
        "32"
    );
}
