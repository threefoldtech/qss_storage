use crate::common::TestServer;
use crate::helpers::*;

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
        "OK (no access without the namespace password)",
        "SELECT reports what it granted rather than refusing -- and on a \
         private namespace it granted nothing, so it must not answer \
         read-only and be refuted by the next GET"
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
        "OK (no access without the namespace password)"
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
