use redis::Connection;

pub(crate) fn nsnew(conn: &mut Connection, name: &str) {
    let created: String = redis::cmd("NSNEW")
        .arg(name)
        .query(conn)
        .expect("the namespace must be created");
    assert_eq!(created, "OK");
}

pub(crate) fn nsset(
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

pub(crate) fn set_property(conn: &mut Connection, name: &str, property: &str, value: &str) {
    let answer = nsset(conn, name, property, value)
        .unwrap_or_else(|e| panic!("NSSET {name} {property} {value} must be accepted: {e}"));
    assert_eq!(answer, "OK");
}

pub(crate) fn nsinfo(conn: &mut Connection, name: &str) -> String {
    redis::cmd("NSINFO")
        .arg(name)
        .query(conn)
        .expect("NSINFO must answer")
}

/// The one field of an NSINFO report, by name.
pub(crate) fn field<'a>(info: &'a str, name: &str) -> &'a str {
    info.lines()
        .find_map(|line| line.strip_prefix(&format!("{name}: ")))
        .unwrap_or_else(|| panic!("NSINFO must report {name}:\n{info}"))
}

pub(crate) fn select(conn: &mut Connection, name: &str) -> String {
    redis::cmd("SELECT")
        .arg(name)
        .query(conn)
        .expect("SELECT must answer")
}

pub(crate) fn select_with(conn: &mut Connection, name: &str, password: &str) -> String {
    redis::cmd("SELECT")
        .arg(name)
        .arg(password)
        .query(conn)
        .expect("SELECT must answer")
}
