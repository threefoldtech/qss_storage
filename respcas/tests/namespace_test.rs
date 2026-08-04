//! What a namespace is, who may change it, and who may read it.
//!
//! NSSET is the only way a namespace's behaviour is configured and NSINFO is
//! the only way it is read back, so the pair is the whole surface: a property
//! that does not survive the round trip is a property an operator cannot
//! trust. The enforcement half is here too -- a flag NSINFO reports and
//! nothing acts on is worse than no flag.

mod common;

use common::TestServer;
use redis::Connection;

fn nsnew(conn: &mut Connection, name: &str) {
    let created: String = redis::cmd("NSNEW")
        .arg(name)
        .query(conn)
        .expect("the namespace must be created");
    assert_eq!(created, "OK");
}

fn nsset(
    conn: &mut Connection,
    name: &str,
    property: &str,
    value: &str,
) -> redis::RedisResult<String> {
    redis::cmd("NSSET")
        .arg(name)
        .arg(property)
        .arg(value)
        .query(conn)
}

fn set_property(conn: &mut Connection, name: &str, property: &str, value: &str) {
    let answer = nsset(conn, name, property, value)
        .unwrap_or_else(|e| panic!("NSSET {name} {property} {value} must be accepted: {e}"));
    assert_eq!(answer, "OK");
}

fn nsinfo(conn: &mut Connection, name: &str) -> String {
    redis::cmd("NSINFO")
        .arg(name)
        .query(conn)
        .expect("NSINFO must answer")
}

/// The one field of an NSINFO report, by name.
fn field<'a>(info: &'a str, name: &str) -> &'a str {
    info.lines()
        .find_map(|line| line.strip_prefix(&format!("{name}: ")))
        .unwrap_or_else(|| panic!("NSINFO must report {name}:\n{info}"))
}

fn select(conn: &mut Connection, name: &str) -> String {
    redis::cmd("SELECT")
        .arg(name)
        .query(conn)
        .expect("SELECT must answer")
}

fn select_with(conn: &mut Connection, name: &str, password: &str) -> String {
    redis::cmd("SELECT")
        .arg(name)
        .arg(password)
        .query(conn)
        .expect("SELECT must answer")
}

/// Every property NSSET accepts comes back out of NSINFO, goes back to where
/// it was, and is still there after the daemon has been restarted.
///
/// The restart is the part that matters: these live in the namespace's
/// metadata record, not in the connection and not in the cache, and a
/// property that only existed in memory would pass every other assertion
/// here.
#[test]
fn every_property_nsset_accepts_round_trips_through_nsinfo() {
    let mut server = TestServer::new();
    let data_dir = server.data_dir().to_path_buf();

    {
        let mut conn = server.connect();
        nsnew(&mut conn, "configured");

        // What a fresh namespace is.
        let info = nsinfo(&mut conn, "configured");
        assert_eq!(field(&info, "name"), "configured");
        assert_eq!(field(&info, "public"), "yes");
        assert_eq!(field(&info, "password"), "no");
        assert_eq!(field(&info, "worm"), "no");
        assert_eq!(field(&info, "locked"), "no");
        assert_eq!(field(&info, "mode"), "userkey");

        assert_eq!(field(&info, "data_limits_bytes"), "0");
        assert_eq!(field(&info, "data_size_bytes"), "0");

        // Every knob, moved.
        set_property(&mut conn, "configured", "worm", "1");
        set_property(&mut conn, "configured", "lock", "1");
        set_property(&mut conn, "configured", "public", "0");
        set_property(&mut conn, "configured", "password", "s3cret");
        set_property(&mut conn, "configured", "key_mode", "cas");
        set_property(&mut conn, "configured", "max_size", "65536");

        let info = nsinfo(&mut conn, "configured");
        assert_eq!(field(&info, "public"), "no");
        assert_eq!(field(&info, "password"), "yes", "reported, never echoed");
        assert_eq!(field(&info, "worm"), "yes");
        assert_eq!(field(&info, "locked"), "yes");
        assert_eq!(field(&info, "mode"), "cas");
        assert_eq!(field(&info, "data_limits_bytes"), "65536");

        // Only 0 and 1 are booleans here.
        for value in ["", "yes", "true", "2", "-1"] {
            let err = nsset(&mut conn, "configured", "worm", value)
                .expect_err("a boolean property takes 0 or 1");
            assert!(
                format!("{err}").contains("Invalid property value"),
                "{value:?}: {err}"
            );
        }
        // And a property that does not exist is named in the refusal.
        let err = nsset(&mut conn, "configured", "nonsense", "1")
            .expect_err("an unknown property must be refused");
        assert!(
            format!("{err}").contains("Unknown property: nonsense"),
            "{err}"
        );
    }

    // The daemon goes away, and the namespace is still what it was told to be.
    server.stop();
    let restarted = TestServer::reopen(&data_dir);
    let mut conn = restarted.connect();

    let info = nsinfo(&mut conn, "configured");
    assert_eq!(field(&info, "public"), "no");
    assert_eq!(field(&info, "password"), "yes");
    assert_eq!(field(&info, "worm"), "yes");
    assert_eq!(field(&info, "locked"), "yes");
    assert_eq!(field(&info, "mode"), "cas");
    assert_eq!(field(&info, "data_limits_bytes"), "65536");

    // And every one of them goes back.
    set_property(&mut conn, "configured", "worm", "0");
    set_property(&mut conn, "configured", "lock", "0");
    set_property(&mut conn, "configured", "public", "1");
    set_property(&mut conn, "configured", "password", "");
    set_property(&mut conn, "configured", "key_mode", "userkey");
    set_property(&mut conn, "configured", "max_size", "0");

    let info = nsinfo(&mut conn, "configured");
    assert_eq!(field(&info, "public"), "yes");
    assert_eq!(field(&info, "password"), "no", "an empty value removes it");
    assert_eq!(field(&info, "worm"), "no");
    assert_eq!(field(&info, "locked"), "no");
    assert_eq!(field(&info, "mode"), "userkey");
    assert_eq!(
        field(&info, "data_limits_bytes"),
        "0",
        "and zero is how a limit is taken off"
    );
}

/// The key-mode gate is not a one-way door with a hole in it: a namespace
/// that has been written to cannot leave the cas mode either.
///
/// The other direction, and the empty-namespace case, are pinned in
/// `cas_test`. What is here is the half that would let a hash key end up in a
/// namespace where nothing means anything.
#[test]
fn a_written_cas_namespace_cannot_leave_the_mode_either() {
    let server = TestServer::new();
    let mut conn = server.connect();

    nsnew(&mut conn, "addressed");
    set_property(&mut conn, "addressed", "key_mode", "cas");
    select(&mut conn, "addressed");

    let key: Vec<u8> = redis::cmd("CSET")
        .arg(b"one record is enough")
        .query(&mut conn)
        .expect("the namespace stores by address");

    let err = nsset(&mut conn, "addressed", "key_mode", "userkey")
        .expect_err("a populated namespace must refuse the change");
    let message = format!("{err}");
    assert!(message.contains("1 key"), "{message}");
    assert!(message.contains("empty"), "{message}");
    assert_eq!(field(&nsinfo(&mut conn, "addressed"), "mode"), "cas");

    // Emptied, it may leave -- and then a 32-byte key is just a key again.
    let removed: i64 = redis::cmd("DEL").arg(&key).query(&mut conn).unwrap();
    assert_eq!(removed, 1);
    set_property(&mut conn, "addressed", "key_mode", "userkey");
    let stored: String = redis::cmd("SET")
        .arg(&key)
        .arg(b"a name now, not an address")
        .query(&mut conn)
        .expect("a user-keyed namespace stores what it is told");
    assert_eq!(stored, "OK");
}

/// A namespace that is not public refuses to be read at all until the
/// password is given. SELECT still succeeds -- it says what it granted.
#[test]
fn a_private_namespace_refuses_reads_until_the_password_is_given() {
    let server = TestServer::new();
    let mut admin = server.connect();

    nsnew(&mut admin, "private");
    select(&mut admin, "private");
    let _: String = redis::cmd("SET")
        .arg("seed")
        .arg("the value behind the door")
        .query(&mut admin)
        .expect("the namespace is writable before it is locked down");
    set_property(&mut admin, "private", "password", "s3cret");
    set_property(&mut admin, "private", "public", "0");

    // A client that does not know the password.
    let mut client = server.connect();
    assert_eq!(
        select(&mut client, "private"),
        "OK (read-only access)",
        "SELECT reports what it granted rather than refusing"
    );

    let err = redis::cmd("GET")
        .arg("seed")
        .query::<Option<String>>(&mut client)
        .expect_err("a private namespace is not readable unauthenticated");
    assert!(
        format!("{err}").contains("Authentication required for read operations"),
        "{err}"
    );
    let err = redis::cmd("EXISTS")
        .arg("seed")
        .query::<i64>(&mut client)
        .expect_err("nor probeable");
    assert!(
        format!("{err}").contains("Authentication required for read operations"),
        "{err}"
    );
    let err = redis::cmd("SET")
        .arg("seed")
        .arg("mine now")
        .query::<String>(&mut client)
        .expect_err("nor writable");
    assert!(
        format!("{err}").contains("Authentication required for write operations"),
        "{err}"
    );

    // With the password, on the same connection.
    assert_eq!(select_with(&mut client, "private", "s3cret"), "OK");
    let value: String = redis::cmd("GET").arg("seed").query(&mut client).unwrap();
    assert_eq!(value, "the value behind the door");
    let _: String = redis::cmd("SET")
        .arg("added")
        .arg("by an authenticated client")
        .query(&mut client)
        .expect("and writable");

    // A wrong password is the same as no password.
    let mut wrong = server.connect();
    assert_eq!(
        select_with(&mut wrong, "private", "not-the-password"),
        "OK (read-only access)"
    );
    assert!(
        redis::cmd("GET")
            .arg("seed")
            .query::<Option<String>>(&mut wrong)
            .is_err()
    );
}

/// A password on a namespace that stays public guards the writes and leaves
/// the reads open -- the two flags are independent, and the pairing is what
/// makes a published read-only namespace expressible.
#[test]
fn a_password_on_a_public_namespace_guards_only_the_writes() {
    let server = TestServer::new();
    let mut admin = server.connect();

    nsnew(&mut admin, "published");
    select(&mut admin, "published");
    let _: String = redis::cmd("SET")
        .arg("seed")
        .arg("readable by anyone")
        .query(&mut admin)
        .unwrap();
    set_property(&mut admin, "published", "password", "s3cret");

    let mut reader = server.connect();
    assert_eq!(select(&mut reader, "published"), "OK (read-only access)");

    let value: String = redis::cmd("GET").arg("seed").query(&mut reader).unwrap();
    assert_eq!(value, "readable by anyone", "public means readable");

    let err = redis::cmd("SET")
        .arg("seed")
        .arg("not by anyone")
        .query::<String>(&mut reader)
        .expect_err("a password still guards the writes");
    assert!(
        format!("{err}").contains("Authentication required for write operations"),
        "{err}"
    );
    let err = redis::cmd("DEL")
        .arg("seed")
        .query::<i64>(&mut reader)
        .expect_err("and the deletes");
    assert!(
        format!("{err}").contains("Authentication required for write operations"),
        "{err}"
    );
}

/// Which commands need the admin password and which do not, on a daemon that
/// has one.
#[test]
fn the_namespace_commands_split_along_the_admin_line() {
    let server = TestServer::new_with_admin(Some("admin123".to_string()));
    let mut client = server.connect();

    // Reading the shape of the store needs nothing.
    let namespaces: Vec<String> = redis::cmd("NSLIST")
        .query(&mut client)
        .expect("NSLIST is open to everyone");
    assert!(namespaces.contains(&"default".to_string()));
    assert!(nsinfo(&mut client, "default").contains("name: default"));

    // Changing it needs the password.
    let err = redis::cmd("NSNEW")
        .arg("mine")
        .query::<String>(&mut client)
        .expect_err("NSNEW is an admin command");
    assert!(format!("{err}").contains("admin privileges"), "{err}");
    let err = nsset(&mut client, "default", "worm", "1").expect_err("so is NSSET");
    assert!(format!("{err}").contains("admin privileges"), "{err}");
    let err = nsset(&mut client, "default", "key_mode", "cas")
        .expect_err("and so is the key mode, which takes its own path");
    assert!(format!("{err}").contains("admin privileges"), "{err}");
    assert_eq!(field(&nsinfo(&mut client, "default"), "worm"), "no");
    assert_eq!(field(&nsinfo(&mut client, "default"), "mode"), "userkey");

    // A wrong password changes nothing about the connection.
    let err = redis::cmd("AUTH")
        .arg("hunter2")
        .query::<String>(&mut client)
        .expect_err("a wrong admin password must be refused");
    assert!(format!("{err}").contains("invalid password"), "{err}");
    assert!(
        redis::cmd("NSNEW")
            .arg("mine")
            .query::<String>(&mut client)
            .is_err(),
        "a failed AUTH leaves the connection where it was"
    );

    // The right one does.
    let authed: String = redis::cmd("AUTH")
        .arg("admin123")
        .query(&mut client)
        .unwrap();
    assert_eq!(authed, "OK");
    nsnew(&mut client, "mine");
    set_property(&mut client, "mine", "worm", "1");

    // And it is this connection's, not the daemon's.
    let mut other = server.connect();
    assert!(
        redis::cmd("NSNEW")
            .arg("theirs")
            .query::<String>(&mut other)
            .is_err(),
        "AUTH is per-connection"
    );
}

/// The two authentications are orthogonal: AUTH neither revokes nor grants
/// what a `SELECT <ns> <password>` decided.
///
/// AUTH rebuilds the connection's handler, and the rebuild used to recompute
/// namespace authentication from whether the namespace has a password at all
/// -- which for a namespace that has one is "not authenticated". So
/// authenticating as ADMIN silently revoked the access a SELECT had granted
/// and the connection's writes started being refused. Both directions are
/// pinned here: what SELECT granted survives, and what it did not grant is
/// not conjured up by being admin.
#[test]
fn authenticating_as_admin_leaves_the_namespace_authentication_where_it_was() {
    let server = TestServer::new_with_admin(Some("admin123".to_string()));
    let mut conn = server.connect();

    let _: String = redis::cmd("AUTH").arg("admin123").query(&mut conn).unwrap();
    nsnew(&mut conn, "guarded");
    set_property(&mut conn, "guarded", "password", "s3cret");

    assert_eq!(select_with(&mut conn, "guarded", "s3cret"), "OK");
    let _: String = redis::cmd("SET")
        .arg("before")
        .arg("written while authenticated")
        .query(&mut conn)
        .expect("the namespace password was given, so writes land");

    // AUTH again, with the same admin password that is already in force.
    let authed: String = redis::cmd("AUTH").arg("admin123").query(&mut conn).unwrap();
    assert_eq!(authed, "OK");

    let _: String = redis::cmd("SET")
        .arg("after")
        .arg("written after re-authenticating")
        .query(&mut conn)
        .expect("AUTH must not revoke what SELECT granted");
    let value: String = redis::cmd("GET").arg("before").query(&mut conn).unwrap();
    assert_eq!(value, "written while authenticated");

    // The other direction, on a connection that never gave the namespace
    // password: being admin is not being authenticated for a namespace.
    let mut other = server.connect();
    assert_eq!(select(&mut other, "guarded"), "OK (read-only access)");
    let authed: String = redis::cmd("AUTH")
        .arg("admin123")
        .query(&mut other)
        .unwrap();
    assert_eq!(authed, "OK");
    let err = redis::cmd("SET")
        .arg("by-the-admin")
        .arg("without the namespace password")
        .query::<String>(&mut other)
        .expect_err("admin is not a namespace password");
    assert!(
        format!("{err}").contains("Authentication required for write operations"),
        "{err}"
    );

    // And it is still one SELECT away.
    assert_eq!(select_with(&mut other, "guarded", "s3cret"), "OK");
    let _: String = redis::cmd("SET")
        .arg("by-the-admin")
        .arg("with the namespace password")
        .query(&mut other)
        .expect("the password is what grants it, whoever is asking");
}

/// What AUTH leaves behind: the namespace access a SELECT already granted on
/// this connection.
#[test]
fn authenticating_as_admin_should_keep_the_namespace_authentication() {
    let server = TestServer::new_with_admin(Some("admin123".to_string()));
    let mut conn = server.connect();

    let _: String = redis::cmd("AUTH").arg("admin123").query(&mut conn).unwrap();
    nsnew(&mut conn, "guarded");
    set_property(&mut conn, "guarded", "password", "s3cret");
    assert_eq!(select_with(&mut conn, "guarded", "s3cret"), "OK");

    let _: String = redis::cmd("AUTH").arg("admin123").query(&mut conn).unwrap();
    let _: String = redis::cmd("SET")
        .arg("after")
        .arg("still authenticated for this namespace")
        .query(&mut conn)
        .expect("AUTH must not revoke what SELECT granted");
}

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

/// `max_size` is a property like the others: set by NSSET, reported by
/// NSINFO as `data_limits_bytes`, and still there after a restart.
///
/// `0` is how an unbounded namespace has always been reported, so it is also
/// how a limit is taken off. Only that one spelling is a property; the others
/// an operator might guess are still refused by name.
#[test]
fn a_namespace_size_limit_is_set_reported_and_removed() {
    let mut server = TestServer::new();
    let data_dir = server.data_dir().to_path_buf();

    {
        let mut conn = server.connect();
        nsnew(&mut conn, "bounded");
        assert_eq!(
            field(&nsinfo(&mut conn, "bounded"), "data_limits_bytes"),
            "0",
            "a namespace is unbounded until it is told otherwise"
        );

        set_property(&mut conn, "bounded", "max_size", "1048576");
        assert_eq!(
            field(&nsinfo(&mut conn, "bounded"), "data_limits_bytes"),
            "1048576"
        );

        // A number of bytes, or nothing.
        for value in ["", "lots", "-1", "1MiB", "1.5"] {
            let err = nsset(&mut conn, "bounded", "max_size", value)
                .expect_err("a size limit is a number of bytes");
            assert!(
                format!("{err}").contains("Invalid property value"),
                "{value:?}: {err}"
            );
        }
        // And the spellings that are not the property are still not it.
        for property in ["maxsize", "data_limits_bytes", "data_size_bytes"] {
            let err = nsset(&mut conn, "bounded", property, "1024")
                .expect_err("only max_size is the property");
            assert!(
                format!("{err}").contains(&format!("Unknown property: {property}")),
                "{err}"
            );
        }
        assert_eq!(
            field(&nsinfo(&mut conn, "bounded"), "data_limits_bytes"),
            "1048576",
            "a refused NSSET changed nothing"
        );
    }

    // The limit is in the namespace's record, not in the daemon.
    server.stop();
    let restarted = TestServer::reopen(&data_dir);
    let mut conn = restarted.connect();
    assert_eq!(
        field(&nsinfo(&mut conn, "bounded"), "data_limits_bytes"),
        "1048576"
    );

    // Zero takes it off again.
    set_property(&mut conn, "bounded", "max_size", "0");
    assert_eq!(
        field(&nsinfo(&mut conn, "bounded"), "data_limits_bytes"),
        "0"
    );
}

/// A write that would take a namespace past its `max_size` is refused, and
/// refused before anything is stored.
///
/// The refusal names the limit, because "no" without a number is not
/// something an operator or a client can act on. An overwrite spends only the
/// difference, so a namespace at its limit can still be written smaller, and
/// a delete makes room the next write can use.
#[test]
fn a_write_over_the_limit_is_refused_and_stores_nothing() {
    let server = TestServer::new();
    let mut conn = server.connect();

    nsnew(&mut conn, "bounded");
    set_property(&mut conn, "bounded", "max_size", "1024");
    select(&mut conn, "bounded");

    let _: String = redis::cmd("SET")
        .arg("first")
        .arg(vec![1u8; 600])
        .query(&mut conn)
        .expect("under the limit");
    assert_eq!(
        field(&nsinfo(&mut conn, "bounded"), "data_size_bytes"),
        "600"
    );

    let err = redis::cmd("SET")
        .arg("second")
        .arg(vec![2u8; 500])
        .query::<String>(&mut conn)
        .expect_err("600 + 500 is over 1024");
    let message = format!("{err}");
    assert!(message.contains("max_size"), "{message}");
    assert!(message.contains("1024"), "the limit is named: {message}");

    // Nothing was written on the way to the refusal.
    let size: i64 = redis::cmd("DBSIZE").query(&mut conn).unwrap();
    assert_eq!(size, 1);
    assert!(
        redis::cmd("GET")
            .arg("second")
            .query::<Option<Vec<u8>>>(&mut conn)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        field(&nsinfo(&mut conn, "bounded"), "data_size_bytes"),
        "600",
        "and the ledger did not move either"
    );

    // Exactly at the limit is allowed; one byte more is not.
    let _: String = redis::cmd("SET")
        .arg("second")
        .arg(vec![2u8; 424])
        .query(&mut conn)
        .expect("600 + 424 is exactly 1024");
    assert_eq!(
        field(&nsinfo(&mut conn, "bounded"), "data_size_bytes"),
        "1024"
    );
    assert!(
        redis::cmd("SET")
            .arg("third")
            .arg(vec![3u8; 1])
            .query::<String>(&mut conn)
            .is_err(),
        "a full namespace is full"
    );

    // An overwrite spends the difference, so writing smaller always works.
    let _: String = redis::cmd("SET")
        .arg("first")
        .arg(vec![1u8; 24])
        .query(&mut conn)
        .expect("shrinking a record cannot break a limit");
    assert_eq!(
        field(&nsinfo(&mut conn, "bounded"), "data_size_bytes"),
        "448"
    );

    // A delete makes room the next write can use.
    let removed: i64 = redis::cmd("DEL").arg("second").query(&mut conn).unwrap();
    assert_eq!(removed, 1);
    let _: String = redis::cmd("SET")
        .arg("third")
        .arg(vec![3u8; 1000])
        .query(&mut conn)
        .expect("24 + 1000 fits");

    // Raising the limit makes room too, and lowering it below what is already
    // there refuses the next write rather than deleting anything.
    set_property(&mut conn, "bounded", "max_size", "100");
    let size: i64 = redis::cmd("DBSIZE").query(&mut conn).unwrap();
    assert_eq!(size, 2, "a lowered limit removes nothing");
    assert!(
        redis::cmd("SET")
            .arg("fourth")
            .arg(vec![4u8; 1])
            .query::<String>(&mut conn)
            .is_err()
    );
    set_property(&mut conn, "bounded", "max_size", "0");
    let _: String = redis::cmd("SET")
        .arg("fourth")
        .arg(vec![4u8; 4096])
        .query(&mut conn)
        .expect("no limit, no refusal");
}

/// The limit in a content-addressed namespace: a write of new content is
/// refused, an acknowledgement of content that is already there is not, and a
/// clone is charged in full (ADR 0014).
#[test]
fn a_content_addressed_namespace_spends_its_limit_on_new_content_only() {
    let server = TestServer::new();
    let mut conn = server.connect();

    for name in ["first", "second"] {
        nsnew(&mut conn, name);
        set_property(&mut conn, name, "key_mode", "cas");
    }
    set_property(&mut conn, "second", "max_size", "4096");

    let value = vec![0x22u8; 4096];
    select(&mut conn, "first");
    let key: Vec<u8> = redis::cmd("CSET").arg(&value).query(&mut conn).unwrap();

    // The bounded namespace takes it: a clone, charged its full size.
    select(&mut conn, "second");
    let _: String = redis::cmd("SET")
        .arg(&key)
        .arg(&value)
        .query(&mut conn)
        .expect("4096 is exactly the limit");
    assert_eq!(
        field(&nsinfo(&mut conn, "second"), "data_size_bytes"),
        "4096"
    );

    // Full: new content is refused, whichever spelling asks for it.
    let err = redis::cmd("CSET")
        .arg(vec![0x33u8; 64])
        .query::<Vec<u8>>(&mut conn)
        .expect_err("the namespace is full");
    assert!(format!("{err}").contains("max_size"), "{err}");
    let err = redis::cmd("SET")
        .arg("")
        .arg(vec![0x44u8; 64])
        .query::<Vec<u8>>(&mut conn)
        .expect_err("and by the other spelling too");
    assert!(format!("{err}").contains("max_size"), "{err}");

    // But content it already holds is still acknowledged: nothing is stored,
    // so there is nothing to refuse -- which is what keeps the
    // probe-then-store workflow working against a full namespace.
    let same: Vec<u8> = redis::cmd("CSET")
        .arg(&value)
        .query(&mut conn)
        .expect("a dedup hit stores nothing");
    assert_eq!(same, key);

    // A clone that would not fit is refused before a reference is taken.
    let big = vec![0x55u8; 8192];
    select(&mut conn, "first");
    let big_key: Vec<u8> = redis::cmd("CSET").arg(&big).query(&mut conn).unwrap();
    select(&mut conn, "second");
    let err = redis::cmd("SET")
        .arg(&big_key)
        .arg(&big)
        .query::<String>(&mut conn)
        .expect_err("a clone is a store on the ledger");
    assert!(format!("{err}").contains("max_size"), "{err}");
    assert_eq!(
        field(&nsinfo(&mut conn, "second"), "data_size_bytes"),
        "4096",
        "the refused clone left the ledger alone"
    );
    let held: i64 = redis::cmd("EXISTS").arg(&big_key).query(&mut conn).unwrap();
    assert_eq!(held, 0, "and stored no record");
}

/// The order the refusals come in: a locked or worm namespace says so before
/// it says anything about a limit.
///
/// Both were refusals before `max_size` existed, and a client that has been
/// told "the namespace is read-only" must not start being told "it is full"
/// instead -- the two mean different things and call for different actions.
#[test]
fn the_lock_and_worm_refusals_come_before_the_limit() {
    let server = TestServer::new();
    let mut conn = server.connect();

    nsnew(&mut conn, "shut");
    set_property(&mut conn, "shut", "max_size", "1");
    set_property(&mut conn, "shut", "lock", "1");
    select(&mut conn, "shut");

    let err = redis::cmd("SET")
        .arg("k")
        .arg(vec![0u8; 4096])
        .query::<String>(&mut conn)
        .expect_err("a locked namespace refuses");
    let message = format!("{err}");
    assert!(message.contains("locked"), "{message}");
    assert!(!message.contains("max_size"), "{message}");

    set_property(&mut conn, "shut", "lock", "0");
    set_property(&mut conn, "shut", "max_size", "8192");
    let _: String = redis::cmd("SET")
        .arg("k")
        .arg(vec![0u8; 4096])
        .query(&mut conn)
        .expect("unlocked and within the limit");

    set_property(&mut conn, "shut", "worm", "1");
    let err = redis::cmd("SET")
        .arg("k")
        .arg(vec![0u8; 8192])
        .query::<String>(&mut conn)
        .expect_err("worm refuses a key it already holds");
    let message = format!("{err}");
    assert!(message.contains("worm"), "{message}");
    assert!(!message.contains("max_size"), "{message}");
}
