use crate::common::TestServer;
use crate::helpers::*;

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
