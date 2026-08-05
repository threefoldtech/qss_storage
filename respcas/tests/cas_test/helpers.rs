use crate::common::{RawConn, simple};
use redis::Connection;

/// Creates `name` and switches it to the content-addressed key mode.
pub(crate) fn cas_namespace(conn: &mut Connection, name: &str) {
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

pub(crate) fn nsinfo(conn: &mut Connection, name: &str) -> String {
    redis::cmd("NSINFO")
        .arg(name)
        .query(conn)
        .expect("NSINFO must answer")
}

/// Creates `name`, switches it to cas, and selects it on this connection.
pub(crate) fn select_cas(conn: &mut Connection, name: &str) {
    cas_namespace(conn, name);
    let _: String = redis::cmd("SELECT")
        .arg(name)
        .query(conn)
        .expect("the namespace must be selectable");
}

/// The address a client computes for itself, with stock tooling: BLAKE3-256
/// over the whole value.
pub(crate) fn address(value: &[u8]) -> Vec<u8> {
    blake3::hash(value).as_bytes().to_vec()
}

/// An address as the daemon prints it in an error message.
pub(crate) fn hex(key: &[u8]) -> String {
    key.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Creates `name`, switches it to cas, and selects it -- over a raw socket,
/// for the tests whose subject is the reply itself.
pub(crate) fn raw_select_cas(conn: &mut RawConn, name: &str) {
    conn.send_command(&[b"NSNEW", name.as_bytes()]);
    assert_eq!(simple(&conn.reply()), "OK");
    conn.send_command(&[b"NSSET", name.as_bytes(), b"key_mode", b"cas"]);
    assert_eq!(simple(&conn.reply()), "OK");
    conn.send_command(&[b"SELECT", name.as_bytes()]);
    assert_eq!(simple(&conn.reply()), "OK");
}

/// A value big enough to leave the inline threshold behind and take the
/// block write path: several 1 MiB blocks, distinct per tag.
pub(crate) fn big_value(tag: u8) -> Vec<u8> {
    const BLOCK: usize = 1 << 20;
    let mut data = Vec::with_capacity(2 * BLOCK + 4096);
    for chunk in 0..2u8 {
        data.extend(std::iter::repeat_n(tag ^ chunk, BLOCK));
    }
    data.extend(std::iter::repeat_n(tag, 4096));
    data
}

pub(crate) fn set(conn: &mut Connection, key: &[u8], value: &[u8]) -> redis::RedisResult<String> {
    redis::cmd("SET").arg(key).arg(value).query(conn)
}

pub(crate) fn get(conn: &mut Connection, key: &[u8]) -> Option<Vec<u8>> {
    redis::cmd("GET")
        .arg(key)
        .query(conn)
        .expect("GET must answer")
}

pub(crate) fn exists(conn: &mut Connection, key: &[u8]) -> bool {
    redis::cmd("EXISTS")
        .arg(key)
        .query(conn)
        .expect("EXISTS must answer")
}
