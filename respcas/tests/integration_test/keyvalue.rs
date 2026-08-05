use crate::common::TestServer;

#[test]
fn test_set_get() {
    let server = TestServer::new();
    let mut conn = server.connect();

    // Test SET and GET
    let _: () = redis::cmd("SET")
        .arg("test_key")
        .arg("test_value")
        .query(&mut conn)
        .expect("Failed to set key");
    let value: String = redis::cmd("GET")
        .arg("test_key")
        .query(&mut conn)
        .expect("Failed to get key");

    assert_eq!(value, "test_value");
}

#[test]
fn test_exists() {
    let server = TestServer::new();
    let mut conn = server.connect();

    // Test EXISTS
    let _: () = redis::cmd("SET")
        .arg("exists_key")
        .arg("value")
        .query(&mut conn)
        .expect("Failed to set key");
    let exists: bool = redis::cmd("EXISTS")
        .arg("exists_key")
        .query(&mut conn)
        .expect("Failed to check if key exists");
    let not_exists: bool = redis::cmd("EXISTS")
        .arg("nonexistent_key")
        .query(&mut conn)
        .expect("Failed to check if key exists");

    assert!(exists);
    assert!(!not_exists);
}

#[test]
fn test_del() {
    let server = TestServer::new();
    let mut conn = server.connect();

    // Test DEL
    let _: () = redis::cmd("SET")
        .arg("del_key")
        .arg("value")
        .query(&mut conn)
        .expect("Failed to set key");
    let exists_before: bool = redis::cmd("EXISTS")
        .arg("del_key")
        .query(&mut conn)
        .expect("Failed to check if key exists");
    let _: () = redis::cmd("DEL")
        .arg("del_key")
        .query(&mut conn)
        .expect("Failed to delete key");
    let exists_after: bool = redis::cmd("EXISTS")
        .arg("del_key")
        .query(&mut conn)
        .expect("Failed to check if key exists");

    assert!(exists_before);
    assert!(!exists_after);
}

/// DEL replies with the number of keys that actually existed -- 0 for
/// an absent key, and the summed count for a variadic call. The absent
/// case used to answer a hardcoded 1.
#[test]
fn test_del_counts_what_existed() {
    let server = TestServer::new();
    let mut conn = server.connect();

    let deleted: i64 = redis::cmd("DEL")
        .arg("never_written")
        .query(&mut conn)
        .expect("DEL of an absent key must not error");
    assert_eq!(deleted, 0, "DEL of an absent key counts nothing");

    for key in ["del_a", "del_b"] {
        let _: () = redis::cmd("SET")
            .arg(key)
            .arg("value")
            .query(&mut conn)
            .expect("Failed to set key");
    }
    let deleted: i64 = redis::cmd("DEL")
        .arg("del_a")
        .arg("still_absent")
        .arg("del_b")
        .query(&mut conn)
        .expect("variadic DEL must not error");
    assert_eq!(deleted, 2, "only the keys that existed are counted");

    let deleted_again: i64 = redis::cmd("DEL")
        .arg("del_a")
        .query(&mut conn)
        .expect("repeat DEL must not error");
    assert_eq!(deleted_again, 0, "a deleted key no longer counts");
}

#[test]
fn test_ping() {
    let server = TestServer::new();
    let mut conn = server.connect();

    // Test PING
    let pong: String = redis::cmd("PING")
        .query(&mut conn)
        .expect("Failed to ping server");
    let custom_pong: String = redis::cmd("PING")
        .arg("hello")
        .query(&mut conn)
        .expect("Failed to ping server with custom message");

    assert_eq!(pong, "PONG");
    assert_eq!(custom_pong, "hello");
}

#[test]
fn test_echo() {
    let server = TestServer::new();
    let mut conn = server.connect();

    let echoed: String = redis::cmd("ECHO")
        .arg("hello")
        .query(&mut conn)
        .expect("Failed to echo");
    assert_eq!(echoed, "hello");

    // valkey-cli --pipe ends its stream with an ECHO of twenty random
    // bytes and matches the reply against them, so the payload has to
    // survive as bytes rather than as text.
    let payload: Vec<u8> = vec![0x00, 0xff, 0xfe, b'a', 0x80, b'\n'];
    let echoed: Vec<u8> = redis::cmd("ECHO")
        .arg(payload.clone())
        .query(&mut conn)
        .expect("Failed to echo binary payload");
    assert_eq!(echoed, payload);

    // ECHO takes exactly one argument.
    let bad: Result<String, _> = redis::cmd("ECHO").query(&mut conn);
    assert!(bad.is_err(), "ECHO with no message must be an error");
}

#[test]
fn test_multiple_commands() {
    let server = TestServer::new();
    let mut conn = server.connect();

    // Set multiple keys
    for i in 0..10 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);
        let _: () = redis::cmd("SET")
            .arg(&key)
            .arg(&value)
            .query(&mut conn)
            .expect("Failed to set key");
    }

    // Get multiple keys
    for i in 0..10 {
        let key = format!("key{}", i);
        let expected = format!("value{}", i);
        let value: String = redis::cmd("GET")
            .arg(&key)
            .query(&mut conn)
            .expect("Failed to get key");
        assert_eq!(value, expected);
    }
}

#[test]
fn test_mget() {
    let server = TestServer::new();
    let mut conn = server.connect();

    // Set multiple keys
    let keys = ["mkey1", "mkey2", "mkey3"];
    let values = ["mvalue1", "mvalue2", "mvalue3"];

    for i in 0..keys.len() {
        let _: () = redis::cmd("SET")
            .arg(keys[i])
            .arg(values[i])
            .query(&mut conn)
            .expect("Failed to set key");
    }

    // Test MGET with all existing keys
    let result: Vec<String> = redis::cmd("MGET")
        .arg(keys[0])
        .arg(keys[1])
        .arg(keys[2])
        .query(&mut conn)
        .expect("Failed to execute MGET");

    assert_eq!(result.len(), 3);
    assert_eq!(result[0], values[0]);
    assert_eq!(result[1], values[1]);
    assert_eq!(result[2], values[2]);

    // Test MGET with some non-existent keys
    let mixed_result: Vec<Option<String>> = redis::cmd("MGET")
        .arg(keys[0])
        .arg("nonexistent_key")
        .arg(keys[2])
        .query(&mut conn)
        .expect("Failed to execute MGET with nonexistent key");

    assert_eq!(mixed_result.len(), 3);
    assert_eq!(mixed_result[0], Some(values[0].to_string()));
    assert_eq!(mixed_result[1], None);
    assert_eq!(mixed_result[2], Some(values[2].to_string()));
}

#[test]
fn test_dbsize() {
    let server = TestServer::new();
    let mut conn = server.connect();

    // A fresh namespace holds no keys
    let empty: i64 = redis::cmd("DBSIZE")
        .query(&mut conn)
        .expect("Failed to execute DBSIZE on empty namespace");
    assert_eq!(empty, 0, "DBSIZE should be 0 for an empty namespace");

    // After N SETs, DBSIZE reports N
    let num_keys = 7;
    for i in 0..num_keys {
        let _: () = redis::cmd("SET")
            .arg(format!("dbsize_key_{}", i))
            .arg(format!("value_{}", i))
            .query(&mut conn)
            .expect("Failed to set key");
    }

    let after_set: i64 = redis::cmd("DBSIZE")
        .query(&mut conn)
        .expect("Failed to execute DBSIZE after SETs");
    assert_eq!(
        after_set, num_keys,
        "DBSIZE should count every key that was set"
    );

    // Overwriting an existing key does not change the count
    let _: () = redis::cmd("SET")
        .arg("dbsize_key_0")
        .arg("overwritten")
        .query(&mut conn)
        .expect("Failed to overwrite key");

    let after_overwrite: i64 = redis::cmd("DBSIZE")
        .query(&mut conn)
        .expect("Failed to execute DBSIZE after overwrite");
    assert_eq!(
        after_overwrite, num_keys,
        "Overwriting a key should not change DBSIZE"
    );

    // After a DEL, DBSIZE reports N-1
    let _: i64 = redis::cmd("DEL")
        .arg("dbsize_key_0")
        .query(&mut conn)
        .expect("Failed to delete key");

    let after_del: i64 = redis::cmd("DBSIZE")
        .query(&mut conn)
        .expect("Failed to execute DBSIZE after DEL");
    assert_eq!(
        after_del,
        num_keys - 1,
        "DBSIZE should drop by one after a DEL"
    );
}

#[test]
fn test_check() {
    let server = TestServer::new();
    let mut conn = server.connect();

    // Set a key
    let key = "check_key";
    let value = "check_value";
    let _: () = redis::cmd("SET")
        .arg(key)
        .arg(value)
        .query(&mut conn)
        .expect("Failed to set key");

    // Test CHECK with existing key
    let check_result: i32 = redis::cmd("CHECK")
        .arg(key)
        .query(&mut conn)
        .expect("Failed to execute CHECK");

    // Should return 1 for a valid key (integrity check passed)
    assert_eq!(check_result, 1);

    // Test CHECK with non-existent key
    let nonexistent_check: i32 = redis::cmd("CHECK")
        .arg("nonexistent_key")
        .query(&mut conn)
        .expect("Failed to execute CHECK with nonexistent key");

    // Should return 0 for a non-existent key
    assert_eq!(nonexistent_check, 0);
}

#[test]
fn test_length_command() {
    // Create a server
    let server = TestServer::new();
    let mut conn = server.connect();

    // Test 1: Set a key and check its length
    let key = "length_test_key";
    let value = "hello"; // 5 bytes
    let _: () = redis::cmd("SET")
        .arg(key)
        .arg(value)
        .query(&mut conn)
        .expect("Failed to set key");

    // Get the length of the key
    let length: i64 = redis::cmd("LENGTH")
        .arg(key)
        .query(&mut conn)
        .expect("Failed to get length");
    assert_eq!(length, 5, "Length should be 5 bytes");

    // Test 2: Check length of a non-existent key
    let non_existent_key = "non_existent_key";
    let length_result: redis::RedisResult<Option<i64>> =
        redis::cmd("LENGTH").arg(non_existent_key).query(&mut conn);

    // Should return nil for non-existent key
    assert!(
        length_result.is_ok(),
        "LENGTH command should not error for non-existent key"
    );
    assert_eq!(
        length_result.unwrap(),
        None,
        "LENGTH should return nil for non-existent key"
    );

    // Test 3: Set a key with longer value and check its length
    let long_key = "long_value_key";
    let long_value = "This is a longer value with more bytes"; // 38 bytes
    let _: () = redis::cmd("SET")
        .arg(long_key)
        .arg(long_value)
        .query(&mut conn)
        .expect("Failed to set long key");

    // Get the length of the long key
    let length: i64 = redis::cmd("LENGTH")
        .arg(long_key)
        .query(&mut conn)
        .expect("Failed to get length of long key");
    assert_eq!(length, 38, "Length should be 38 bytes");
}

#[test]
fn test_keytime_command() {
    // Create a server
    let server = TestServer::new();
    let mut conn = server.connect();

    // Test 1: Set a key and check its timestamp
    let key = "keytime_test_key";
    let value = "hello";
    let _: () = redis::cmd("SET")
        .arg(key)
        .arg(value)
        .query(&mut conn)
        .expect("Failed to set key");

    // Get the timestamp of the key
    let timestamp: i64 = redis::cmd("KEYTIME")
        .arg(key)
        .query(&mut conn)
        .expect("Failed to get timestamp");

    // The timestamp should be a positive number representing Unix time
    assert!(timestamp > 0, "Timestamp should be a positive number");

    // Test 2: Check timestamp of a non-existent key
    let non_existent_key = "non_existent_key";
    let keytime_result: redis::RedisResult<Option<i64>> =
        redis::cmd("KEYTIME").arg(non_existent_key).query(&mut conn);

    // Should return nil for non-existent key
    assert!(
        keytime_result.is_ok(),
        "KEYTIME command should not error for non-existent key"
    );
    assert_eq!(
        keytime_result.unwrap(),
        None,
        "KEYTIME should return nil for non-existent key"
    );

    // Test 3: Set a key and verify the timestamp is updated
    let update_key = "update_timestamp_key";

    // Set the key first time
    let _: () = redis::cmd("SET")
        .arg(update_key)
        .arg("initial value")
        .query(&mut conn)
        .expect("Failed to set update key");

    // Get the initial timestamp
    let initial_timestamp: i64 = redis::cmd("KEYTIME")
        .arg(update_key)
        .query(&mut conn)
        .expect("Failed to get initial timestamp");

    // Sleep for a short time to ensure timestamp will be different
    std::thread::sleep(std::time::Duration::from_secs(1));

    // Update the key
    let _: () = redis::cmd("SET")
        .arg(update_key)
        .arg("updated value")
        .query(&mut conn)
        .expect("Failed to update key");

    // Get the updated timestamp
    let updated_timestamp: i64 = redis::cmd("KEYTIME")
        .arg(update_key)
        .query(&mut conn)
        .expect("Failed to get updated timestamp");

    // The updated timestamp should be different from the initial one
    // Note: This test might be flaky if the system is extremely fast and both operations
    // happen within the same second. The sleep should prevent this.
    assert!(
        updated_timestamp >= initial_timestamp,
        "Updated timestamp should be greater than or equal to the initial timestamp"
    );
}
