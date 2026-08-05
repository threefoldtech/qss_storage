use crate::common::TestServer;
use crate::helpers::*;

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
