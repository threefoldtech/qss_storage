use crate::common::TestServer;
use crate::helpers::*;

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
