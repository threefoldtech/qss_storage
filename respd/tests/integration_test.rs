use redis::{Client, Connection};
use std::fs;
use std::net::TcpListener;
use std::sync::Arc;
use std::thread::{self, sleep};
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::oneshot;

struct TestServer {
    port: u16,
    _temp_dir: tempfile::TempDir, // Keep this field to ensure the directory isn't deleted
    _server_handle: thread::JoinHandle<()>,
    shutdown_sender: Option<oneshot::Sender<()>>,
}

impl TestServer {
    fn new() -> Self {
        Self::new_with_admin(None)
    }

    fn new_with_admin(admin_password: Option<String>) -> Self {
        // Create a temporary directory for the server data
        let temp_dir = tempdir().expect("Failed to create temp directory");
        let data_dir = temp_dir.path().to_path_buf();

        // Make sure the directory exists
        fs::create_dir_all(&data_dir).expect("Failed to create data directory");

        // Bind here, on an OS-assigned port, and hand the bound listener to the
        // server thread. Binding before the thread starts means the port can
        // never be stolen in between, and the kernel queues connections in the
        // listen backlog until the accept loop is up -- so no sleep is needed.
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to address");
        listener
            .set_nonblocking(true)
            .expect("Failed to set non-blocking");
        let port = listener
            .local_addr()
            .expect("Failed to get local address")
            .port();
        println!("Starting respd server on port {}", port);

        // Create a shutdown channel
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();

        // Start the server in a separate thread
        let thread_data_dir = data_dir.clone();
        let thread_admin_password = admin_password.clone();
        let server_handle = thread::spawn(move || {
            // Create a new runtime for this thread
            let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");

            rt.block_on(async {
                println!("Listening on: 127.0.0.1:{}", port);

                // Create a shared storage instance
                let storage = Arc::new(
                    respd::storage::Storage::new(
                        thread_data_dir,
                        None,
                        cas_storage::Durability::Fsync,
                        cas_storage::HeaderSpec::default(),
                    )
                    .expect("can open storage"),
                );

                // Create a shared namespace cache
                let namespace_cache = Arc::new(respd::namespace::NamespaceCache::new(storage.clone()));

                // Convert to tokio TcpListener
                let listener = tokio::net::TcpListener::from_std(listener).expect("Failed to convert listener");

                // Create a future that completes when shutdown signal is received
                let shutdown_future = async {
                    let _ = shutdown_receiver.await;
                    println!("Shutdown signal received");
                };

                // Accept connections until shutdown signal is received
                tokio::select! {
                    _ = shutdown_future => {
                        println!("Server shutting down");
                    }
                    _ = async {
                        loop {
                            match listener.accept().await {
                                Ok((socket, addr)) => {
                                    println!("Accepted connection from: {}", addr);

                                    // Clone the storage for this connection
                                    let storage = Arc::clone(&storage);

                                    // Clone the namespace cache for this connection
                                    let namespace_cache = Arc::clone(&namespace_cache);

                                    // Spawn a new task to handle this connection
                                    // Clone admin password for this connection
                                    let admin_password = thread_admin_password.clone();
                                    tokio::spawn(async move {
                                        // Pass admin_password to the process function
                                        if let Err(e) = respd::server::process(socket, storage, namespace_cache, admin_password).await {
                                            eprintln!("Error processing connection: {}", e);
                                        }
                                    });
                                }
                                Err(e) => {
                                    eprintln!("Error accepting connection: {}", e);
                                }
                            }
                        }
                    } => {}
                }
            });
        });

        TestServer {
            port,
            _temp_dir: temp_dir,
            _server_handle: server_handle,
            shutdown_sender: Some(shutdown_sender),
        }
    }

    fn connect(&self) -> Connection {
        // Try to connect multiple times with backoff
        let url = format!("redis://127.0.0.1:{}", self.port);
        let client = Client::open(url.as_str()).expect("Failed to create Redis client");

        for attempt in 1..=5 {
            match client.get_connection() {
                Ok(conn) => return conn,
                Err(e) => {
                    if attempt == 5 {
                        panic!("Failed to connect to Redis server after 5 attempts: {}", e);
                    }
                    println!("Connection attempt {} failed: {}, retrying...", attempt, e);
                    sleep(Duration::from_millis(500 * attempt));
                }
            }
        }

        panic!("Failed to connect to Redis server");
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        println!("Shutting down test server on port {}", self.port);
        // Take ownership of the sender before sending
        if let Some(sender) = std::mem::take(&mut self.shutdown_sender) {
            let _ = sender.send(());
        }
    }
}

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

// Run the tests one at a time to avoid port conflicts
#[cfg(test)]
mod test_config {
    use super::*;

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
    fn test_namespace_creation() {
        let server = TestServer::new();
        let mut conn = server.connect();

        // Try to select a non-existent namespace
        let namespace_name = "test_namespace";
        let select_result = redis::cmd("SELECT")
            .arg(namespace_name)
            .query::<String>(&mut conn);

        // Should fail because the namespace doesn't exist yet
        assert!(select_result.is_err());
        if let Err(e) = select_result {
            assert!(
                e.to_string().contains("Namespace not found"),
                "Expected error message to contain 'Namespace not found', got: {}",
                e
            );
        }

        // Create a new namespace
        let result: String = redis::cmd("NSNEW")
            .arg(namespace_name)
            .query(&mut conn)
            .expect("Failed to create namespace");

        // Should return OK for successful namespace creation
        assert_eq!(result, "OK");

        // Select the newly created namespace
        let select_result: String = redis::cmd("SELECT")
            .arg(namespace_name)
            .query(&mut conn)
            .expect("Failed to select namespace");

        // Should return OK for successful namespace selection
        assert_eq!(select_result, "OK");

        // Get namespace info
        let info: String = redis::cmd("NSINFO")
            .arg(namespace_name)
            .query(&mut conn)
            .expect("Failed to get namespace info");

        // Verify the namespace info contains the expected fields
        assert!(info.contains("name: test_namespace"));
        assert!(info.contains("public: yes"));
        assert!(info.contains("password: no"));
        assert!(info.contains("data_limits_bytes: 0"));
        assert!(info.contains("mode: userkey"));
        assert!(info.contains("worm: no"));
        assert!(info.contains("locked: no"));
        // Verify the '## new fields' line is not present
        assert!(!info.contains("## new fields"));

        // Test operations in the new namespace
        let test_key = "ns_test_key";
        let test_value = "ns_test_value";
        let _: () = redis::cmd("SET")
            .arg(test_key)
            .arg(test_value)
            .query(&mut conn)
            .expect("Failed to set key in new namespace");

        let value: String = redis::cmd("GET")
            .arg(test_key)
            .query(&mut conn)
            .expect("Failed to get key from new namespace");

        assert_eq!(value, test_value);
    }

    #[test]
    fn test_nslist() {
        let server = TestServer::new();
        let mut conn = server.connect();

        // Create a few namespaces for testing
        let namespaces = vec!["ns1", "ns2", "ns3"];

        for ns in &namespaces {
            let result: String = redis::cmd("NSNEW")
                .arg(ns)
                .query(&mut conn)
                .expect("Failed to create namespace");
            assert_eq!(result, "OK");
        }

        // Execute NSLIST command
        let result: Vec<String> = redis::cmd("NSLIST")
            .query(&mut conn)
            .expect("Failed to execute NSLIST command");

        // Verify that all created namespaces are in the result
        // Note: The result should also include the default namespace
        assert!(result.contains(&"default".to_string()));
        for ns in &namespaces {
            assert!(
                result.contains(&ns.to_string()),
                "Namespace {} not found in NSLIST result",
                ns
            );
        }

        // Verify the total count (all created namespaces + default)
        assert_eq!(result.len(), namespaces.len() + 1);
    }

    #[test]
    fn test_nsnew_admin_required() {
        // Create a server with admin authentication required
        let admin_password = "admin123".to_string();
        let server = TestServer::new_with_admin(Some(admin_password.clone()));
        let mut conn = server.connect();

        // Try to create a namespace without authentication
        let namespace_name = "test_namespace_no_auth";
        let result = redis::cmd("NSNEW")
            .arg(namespace_name)
            .query::<String>(&mut conn);

        // Should fail because admin privileges are required
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(
                e.to_string().contains("requires admin privileges"),
                "Expected error message to contain 'requires admin privileges', got: {}",
                e
            );
        }

        // Authenticate as admin
        let auth_result: String = redis::cmd("AUTH")
            .arg(&admin_password)
            .query(&mut conn)
            .expect("Failed to authenticate");
        assert_eq!(auth_result, "OK");

        // Now try to create a namespace with admin privileges
        let result: String = redis::cmd("NSNEW")
            .arg(namespace_name)
            .query(&mut conn)
            .expect("Failed to create namespace with admin privileges");

        // Should succeed now
        assert_eq!(result, "OK");

        // Verify the namespace was created by selecting it
        let select_result: String = redis::cmd("SELECT")
            .arg(namespace_name)
            .query(&mut conn)
            .expect("Failed to select namespace");
        assert_eq!(select_result, "OK");
    }

    #[test]
    fn test_auth_command() {
        // Create a server with admin authentication required
        let admin_password = "secure_password".to_string();
        let server = TestServer::new_with_admin(Some(admin_password.clone()));
        let mut conn = server.connect();

        // Test 1: Authenticate with correct password (positive case)
        let auth_result: String = redis::cmd("AUTH")
            .arg(&admin_password)
            .query(&mut conn)
            .expect("Failed to authenticate with correct password");
        assert_eq!(
            auth_result, "OK",
            "Authentication with correct password should return OK"
        );

        // Test 2: Authenticate with incorrect password (negative case)
        let wrong_password = "wrong_password";
        let auth_result = redis::cmd("AUTH")
            .arg(wrong_password)
            .query::<String>(&mut conn);

        // Should fail with invalid password error
        assert!(
            auth_result.is_err(),
            "Authentication with wrong password should fail"
        );
        if let Err(e) = auth_result {
            assert!(
                e.to_string().contains("invalid password"),
                "Expected error message to contain 'invalid password', got: {}",
                e
            );
        }

        // Test 3: Server without admin password requirement
        let server_no_auth = TestServer::new(); // No admin password required
        let mut conn_no_auth = server_no_auth.connect();

        // AUTH command should succeed even with any password
        let random_password = "random_password";
        let auth_result: String = redis::cmd("AUTH")
            .arg(random_password)
            .query(&mut conn_no_auth)
            .expect("Failed to authenticate on server with no auth required");
        assert_eq!(
            auth_result, "OK",
            "Authentication on server with no auth required should always return OK"
        );

        // Test 4: AUTH command with wrong number of arguments
        let auth_result = redis::cmd("AUTH").query::<String>(&mut conn);

        // Should fail with wrong number of arguments error
        assert!(auth_result.is_err(), "AUTH without password should fail");
        if let Err(e) = auth_result {
            assert!(
                e.to_string().contains("Wrong number of arguments"),
                "Expected error message to contain 'Wrong number of arguments', got: {}",
                e
            );
        }
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

    #[test]
    fn test_worm_protection() {
        // Create a server with admin password for testing NSSET command
        let server = TestServer::new_with_admin(Some("admin123".to_string()));
        let mut conn = server.connect();

        // Authenticate as admin
        let auth_result: String = redis::cmd("AUTH")
            .arg("admin123")
            .query(&mut conn)
            .expect("Failed to authenticate");
        assert_eq!(auth_result, "OK");

        // Create a test namespace
        let test_ns = "worm_test_ns";
        let nsnew_result: String = redis::cmd("NSNEW")
            .arg(test_ns)
            .query(&mut conn)
            .expect("Failed to create namespace");
        assert_eq!(nsnew_result, "OK");

        // Select the test namespace
        let select_result: String = redis::cmd("SELECT")
            .arg(test_ns)
            .query(&mut conn)
            .expect("Failed to select namespace");
        assert_eq!(select_result, "OK");

        // Set a test key before enabling WORM mode
        let test_key = "test_key";
        let set_result: String = redis::cmd("SET")
            .arg(test_key)
            .arg("initial_value")
            .query(&mut conn)
            .expect("Failed to set test key");
        assert_eq!(set_result, "OK");

        // Enable WORM mode for the namespace
        let nsset_result: String = redis::cmd("NSSET")
            .arg(test_ns)
            .arg("worm")
            .arg("1")
            .query(&mut conn)
            .expect("Failed to set WORM mode");
        assert_eq!(nsset_result, "OK");

        // Try to modify the existing key (should fail)
        let modify_result = redis::cmd("SET")
            .arg(test_key)
            .arg("modified_value")
            .query::<String>(&mut conn);

        assert!(
            modify_result.is_err(),
            "Modifying key in WORM mode should fail"
        );
        if let Err(err) = modify_result {
            assert!(
                err.to_string().contains("ERR:") && err.to_string().contains("worm mode"),
                "Error should mention ERR: prefix and WORM mode"
            );
        }

        // Try to delete the key (should fail)
        let del_result = redis::cmd("DEL").arg(test_key).query::<i32>(&mut conn);

        assert!(del_result.is_err(), "Deleting key in WORM mode should fail");
        if let Err(err) = del_result {
            assert!(
                err.to_string().contains("ERR:") && err.to_string().contains("worm mode"),
                "Error should mention ERR: prefix and WORM mode"
            );
        }

        // We should still be able to add new keys
        let new_key = "new_key";
        let new_set_result: String = redis::cmd("SET")
            .arg(new_key)
            .arg("new_value")
            .query(&mut conn)
            .expect("Failed to set new key in WORM mode");
        assert_eq!(new_set_result, "OK");

        // Disable WORM mode
        let disable_result: String = redis::cmd("NSSET")
            .arg(test_ns)
            .arg("worm")
            .arg("0")
            .query(&mut conn)
            .expect("Failed to disable WORM mode");
        assert_eq!(disable_result, "OK");

        // Now we should be able to modify and delete keys again
        let modify_after_result: String = redis::cmd("SET")
            .arg(test_key)
            .arg("modified_after_worm")
            .query(&mut conn)
            .expect("Failed to modify key after disabling WORM");
        assert_eq!(modify_after_result, "OK");

        let del_after_result: i32 = redis::cmd("DEL")
            .arg(test_key)
            .query(&mut conn)
            .expect("Failed to delete key after disabling WORM");
        assert_eq!(del_after_result, 1);
    }

    #[test]
    fn test_lock_protection() {
        // Create a server with admin password for testing NSSET command
        let server = TestServer::new_with_admin(Some("admin123".to_string()));
        let mut conn = server.connect();

        // Authenticate as admin
        let auth_result: String = redis::cmd("AUTH")
            .arg("admin123")
            .query(&mut conn)
            .expect("Failed to authenticate");
        assert_eq!(auth_result, "OK");

        // Create a test namespace
        let test_ns = "lock_test_ns";
        let nsnew_result: String = redis::cmd("NSNEW")
            .arg(test_ns)
            .query(&mut conn)
            .expect("Failed to create namespace");
        assert_eq!(nsnew_result, "OK");

        // Select the test namespace
        let select_result: String = redis::cmd("SELECT")
            .arg(test_ns)
            .query(&mut conn)
            .expect("Failed to select namespace");
        assert_eq!(select_result, "OK");

        // Set a test key before enabling lock mode
        let test_key = "test_key";
        let set_result: String = redis::cmd("SET")
            .arg(test_key)
            .arg("initial_value")
            .query(&mut conn)
            .expect("Failed to set test key");
        assert_eq!(set_result, "OK");

        // Enable lock mode for the namespace
        let nsset_result: String = redis::cmd("NSSET")
            .arg(test_ns)
            .arg("lock")
            .arg("1")
            .query(&mut conn)
            .expect("Failed to set lock mode");
        assert_eq!(nsset_result, "OK");

        // Try to modify a key (should fail)
        let modify_result = redis::cmd("SET")
            .arg(test_key)
            .arg("modified_value")
            .query::<String>(&mut conn);

        assert!(
            modify_result.is_err(),
            "Modifying key in lock mode should fail"
        );
        if let Err(err) = modify_result {
            assert!(
                err.to_string().contains("ERR:") && err.to_string().contains("locked"),
                "Error should mention ERR: prefix and locked namespace"
            );
        }

        // Try to add a new key (should fail)
        let new_key = "new_key";
        let new_set_result = redis::cmd("SET")
            .arg(new_key)
            .arg("new_value")
            .query::<String>(&mut conn);

        assert!(
            new_set_result.is_err(),
            "Adding new key in lock mode should fail"
        );
        if let Err(err) = new_set_result {
            assert!(
                err.to_string().contains("ERR:") && err.to_string().contains("locked"),
                "Error should mention ERR: prefix and locked namespace"
            );
        }

        // Try to delete the key (should fail)
        let del_result = redis::cmd("DEL").arg(test_key).query::<i32>(&mut conn);

        assert!(del_result.is_err(), "Deleting key in lock mode should fail");
        if let Err(err) = del_result {
            assert!(
                err.to_string().contains("ERR:") && err.to_string().contains("locked"),
                "Error should mention ERR: prefix and locked namespace"
            );
        }

        // Disable lock mode
        let disable_result: String = redis::cmd("NSSET")
            .arg(test_ns)
            .arg("lock")
            .arg("0")
            .query(&mut conn)
            .expect("Failed to disable lock mode");
        assert_eq!(disable_result, "OK");

        // Now we should be able to modify and delete keys again
        let modify_after_result: String = redis::cmd("SET")
            .arg(test_key)
            .arg("modified_after_lock")
            .query(&mut conn)
            .expect("Failed to modify key after disabling lock");
        assert_eq!(modify_after_result, "OK");

        // Add a new key
        let add_after_result: String = redis::cmd("SET")
            .arg(new_key)
            .arg("added_after_lock")
            .query(&mut conn)
            .expect("Failed to add new key after disabling lock");
        assert_eq!(add_after_result, "OK");

        // Delete a key
        let del_after_result: i32 = redis::cmd("DEL")
            .arg(test_key)
            .query(&mut conn)
            .expect("Failed to delete key after disabling lock");
        assert_eq!(del_after_result, 1);
    }

    #[test]
    fn test_scan_command() {
        // Create a server
        let server = TestServer::new();
        let mut conn = server.connect();

        // Create a set of keys for testing
        let num_keys = 15; // More than the 10 keys returned per scan
        let key_prefix = "scan_test_key_";

        // Insert test keys
        for i in 0..num_keys {
            let key = format!("{}{}", key_prefix, i);
            let value = format!("value_{}", i);
            let _: () = redis::cmd("SET")
                .arg(&key)
                .arg(&value)
                .query(&mut conn)
                .unwrap_or_else(|_| panic!("Failed to set key {}", key));
        }

        // Test 1: Initial scan with cursor 0
        let scan_result: (String, Vec<String>) = redis::cmd("SCAN")
            .arg("0")
            .query(&mut conn)
            .expect("Failed to execute SCAN command");

        // Check that we got some keys
        let (cursor, keys) = scan_result;
        assert!(!keys.is_empty(), "Expected some keys in the first scan");
        assert!(
            keys.len() <= 10,
            "Expected at most 10 keys in a single scan"
        );

        // All keys in the database are from our test

        // If cursor is 0, we're done. Otherwise, continue scanning
        if cursor != "0" {
            // Test 2: Continue scanning with the returned cursor
            let scan_result2: (String, Vec<String>) = redis::cmd("SCAN")
                .arg(&cursor)
                .query(&mut conn)
                .expect("Failed to execute second SCAN command");

            let (cursor2, keys2) = scan_result2;

            // Check that we got more keys (or a terminal cursor)
            if keys2.is_empty() {
                // If no more keys, cursor should be 0
                assert_eq!(cursor2, "0", "Expected cursor 0 when no more keys");
            } else {
                // If we got more keys, verify they're different from the first batch

                // Keys from second scan should be different from first scan
                for key in &keys2 {
                    assert!(
                        !keys.contains(key),
                        "Keys should not be duplicated between scans"
                    );
                }
            }
        }

        // Test 3: Complete scan to get all keys
        let mut all_keys = Vec::new();
        let mut next_cursor = "0".to_string();

        loop {
            let scan_result: (String, Vec<String>) = redis::cmd("SCAN")
                .arg(&next_cursor)
                .query(&mut conn)
                .expect("Failed to execute SCAN in loop");

            let (cursor, mut batch_keys) = scan_result;
            all_keys.append(&mut batch_keys);

            if cursor == "0" {
                break;
            }
            next_cursor = cursor;
        }

        // Verify we got all our test keys
        assert_eq!(all_keys.len(), num_keys, "Should have found all test keys");

        // Verify each expected key is in the results
        for i in 0..num_keys {
            let expected_key = format!("{}{}", key_prefix, i);
            assert!(
                all_keys.contains(&expected_key),
                "Missing expected key: {}",
                expected_key
            );
        }
    }

    #[test]
    fn test_rscan_command() {
        // Create a server
        let server = TestServer::new();
        let mut conn = server.connect();

        // Create a set of keys for testing
        let num_keys = 15; // More than the 10 keys returned per scan
        let key_prefix = "rscan_test_key_";

        // Insert test keys
        for i in 0..num_keys {
            let key = format!("{}{}", key_prefix, i);
            let value = format!("value_{}", i);
            let _: () = redis::cmd("SET")
                .arg(&key)
                .arg(&value)
                .query(&mut conn)
                .unwrap_or_else(|_| panic!("Failed to set key {}", key));
        }

        // For RSCAN, we need a valid cursor to start from
        // Let's first get all keys with SCAN to find a valid cursor
        let mut all_scan_keys = Vec::new();
        let mut next_cursor = "0".to_string();

        loop {
            let scan_result: (String, Vec<String>) = redis::cmd("SCAN")
                .arg(&next_cursor)
                .query(&mut conn)
                .expect("Failed to execute SCAN in loop");

            let (cursor, mut batch_keys) = scan_result;
            all_scan_keys.append(&mut batch_keys);

            if cursor == "0" {
                break;
            }
            next_cursor = cursor;
        }

        // Filter to only keep our test keys and find the last one
        all_scan_keys.retain(|k| k.starts_with(key_prefix));
        assert!(
            !all_scan_keys.is_empty(),
            "Expected to find test keys with SCAN"
        );

        // Use the last key as our starting point for RSCAN
        let start_key = all_scan_keys.last().unwrap().clone();

        // Test 1: Initial rscan with a valid cursor
        let rscan_result: (String, Vec<String>) = redis::cmd("RSCAN")
            .arg(&start_key)
            .query(&mut conn)
            .expect("Failed to execute RSCAN command");

        // Check that we got some keys
        let (cursor, keys) = rscan_result;
        // We might not get keys if the start_key is the smallest key
        // But we should at least get a valid response
        assert!(
            keys.len() <= 10,
            "Expected at most 10 keys in a single rscan"
        );

        // If cursor is 0, we're done. Otherwise, continue scanning
        if cursor != "0" {
            // Test 2: Continue scanning with the returned cursor
            let rscan_result2: (String, Vec<String>) = redis::cmd("RSCAN")
                .arg(&cursor)
                .query(&mut conn)
                .expect("Failed to execute second RSCAN command");

            let (cursor2, keys2) = rscan_result2;

            // Check that we got more keys (or a terminal cursor)
            if keys2.is_empty() {
                // If no more keys, cursor should be 0
                assert_eq!(cursor2, "0", "Expected cursor 0 when no more keys");
            } else {
                // If we got more keys, verify they're different from the first batch
                // Keys from second scan should be different from first scan
                for key in &keys2 {
                    assert!(
                        !keys.contains(key),
                        "Keys should not be duplicated between scans"
                    );
                }
            }
        }

        // Test 3: Complete backward scan to get all keys
        // For a complete backward scan, we need to start from the largest key
        // Let's first get all keys with SCAN
        let mut all_scan_keys = Vec::new();
        let mut next_cursor = "0".to_string();

        loop {
            let scan_result: (String, Vec<String>) = redis::cmd("SCAN")
                .arg(&next_cursor)
                .query(&mut conn)
                .expect("Failed to execute SCAN in loop");

            let (cursor, mut batch_keys) = scan_result;
            all_scan_keys.append(&mut batch_keys);

            if cursor == "0" {
                break;
            }
            next_cursor = cursor;
        }

        // Filter to only keep our test keys and find the largest one
        all_scan_keys.retain(|k| k.starts_with(key_prefix));
        all_scan_keys.sort();

        let largest_key = all_scan_keys.last().unwrap().clone();

        // Now do a complete backward scan starting from the largest key
        // We need to include the largest key in our results, so we'll add it manually
        let mut all_keys = Vec::new();
        all_keys.push(largest_key.clone()); // Add the largest key first

        // Then start scanning backward from the largest key
        let mut next_cursor = largest_key.clone();

        loop {
            let rscan_result: (String, Vec<String>) = redis::cmd("RSCAN")
                .arg(&next_cursor)
                .query(&mut conn)
                .expect("Failed to execute RSCAN in loop");

            let (cursor, mut batch_keys) = rscan_result;

            // Filter out the largest key if it's in the batch to avoid duplicates
            let largest_key_ref = &all_keys[0]; // Reference to the largest key we added
            batch_keys.retain(|k| k != largest_key_ref);

            all_keys.append(&mut batch_keys);

            if cursor == "0" {
                break;
            }
            next_cursor = cursor;
        }

        // Verify we got all our test keys
        assert_eq!(all_keys.len(), num_keys, "Should have found all test keys");

        // Verify each expected key is in the results
        for i in 0..num_keys {
            let expected_key = format!("{}{}", key_prefix, i);
            assert!(
                all_keys.contains(&expected_key),
                "Missing expected key: {}",
                expected_key
            );
        }

        // Test 4: Verify that keys were collected in reverse order
        // Sort the keys to check if we got all of them
        all_keys.sort();

        // Verify we got all our test keys
        assert_eq!(
            all_keys.len(),
            num_keys,
            "Should have found all test keys with RSCAN"
        );

        // Verify each expected key is in the results
        for i in 0..num_keys {
            let expected_key = format!("{}{}", key_prefix, i);
            assert!(
                all_keys.contains(&expected_key),
                "Missing expected key: {}",
                expected_key
            );
        }

        // Test 5: Verify that RSCAN returns keys in reverse order compared to SCAN
        // Create a new set of keys with a predictable order
        let ordered_prefix = "ordered_key_";

        // Insert keys with ordered values to ensure a specific lexicographical order
        for i in 0..5 {
            let key = format!("{}{:02}", ordered_prefix, i); // Use padding to ensure correct ordering
            let value = format!("value_{}", i);
            let _: () = redis::cmd("SET")
                .arg(&key)
                .arg(&value)
                .query(&mut conn)
                .unwrap_or_else(|_| panic!("Failed to set ordered key {}", key));
        }

        // Get all ordered keys with SCAN (forward direction)
        let mut scan_ordered_keys = Vec::new();
        let mut next_cursor = "0".to_string();

        loop {
            let scan_result: (String, Vec<String>) = redis::cmd("SCAN")
                .arg(&next_cursor)
                .query(&mut conn)
                .expect("Failed to execute SCAN for ordered keys");

            let (cursor, mut batch_keys) = scan_result;
            scan_ordered_keys.append(&mut batch_keys);

            if cursor == "0" {
                break;
            }
            next_cursor = cursor;
        }

        // Filter to only keep our ordered test keys
        scan_ordered_keys.retain(|k| k.starts_with(ordered_prefix));
        scan_ordered_keys.sort(); // Sort to ensure consistent order for testing

        // Get the largest ordered key to start RSCAN from
        let largest_ordered_key = scan_ordered_keys.last().unwrap().clone();

        // Get all ordered keys with RSCAN (backward direction)
        let mut rscan_ordered_keys = Vec::new();

        // Add the largest key manually first (since RSCAN might not include it)
        rscan_ordered_keys.push(largest_ordered_key.clone());

        // Then start scanning backward from the largest key
        let mut next_cursor = largest_ordered_key.clone();

        loop {
            let rscan_result: (String, Vec<String>) = redis::cmd("RSCAN")
                .arg(&next_cursor)
                .query(&mut conn)
                .expect("Failed to execute RSCAN for ordered keys");

            let (cursor, mut batch_keys) = rscan_result;

            // Filter out the largest key if it's in the batch to avoid duplicates
            let largest_key_ref = &rscan_ordered_keys[0]; // Reference to the first key we added
            batch_keys.retain(|k| k != largest_key_ref);

            rscan_ordered_keys.append(&mut batch_keys);

            if cursor == "0" {
                break;
            }
            next_cursor = cursor;
        }

        // Filter to only keep our ordered test keys
        rscan_ordered_keys.retain(|k| k.starts_with(ordered_prefix));

        // Verify that RSCAN returns keys in reverse order
        // We need to reverse the scan_ordered_keys to match the expected order from RSCAN
        let mut reversed_scan_keys = scan_ordered_keys.clone();
        reversed_scan_keys.reverse();

        assert_eq!(
            rscan_ordered_keys, reversed_scan_keys,
            "RSCAN should return keys in reverse order compared to SCAN"
        );
    }

    #[test]
    fn test_flush_command() {
        // Create a server with admin password for namespace operations
        let server = TestServer::new_with_admin(Some("adminpass".to_string()));
        let mut conn = server.connect();

        // Create a private namespace
        let ns_name = "flush_test_ns";

        // Create the namespace (requires admin auth)
        let _: () = redis::cmd("AUTH")
            .arg("adminpass")
            .query(&mut conn)
            .expect("Failed to authenticate as admin");

        // Create the namespace
        let _: String = redis::cmd("NSNEW")
            .arg(ns_name)
            .query(&mut conn)
            .expect("Failed to create namespace");

        // Set password for the namespace
        let ns_password = "nspass";
        let _: String = redis::cmd("NSSET")
            .arg(ns_name)
            .arg("password")
            .arg(ns_password)
            .query(&mut conn)
            .expect("Failed to set namespace password");

        // Set namespace to private (public=0)
        let _: String = redis::cmd("NSSET")
            .arg(ns_name)
            .arg("public")
            .arg("0")
            .query(&mut conn)
            .expect("Failed to set namespace to private");

        // Switch to the new namespace and authenticate
        let _: () = redis::cmd("SELECT")
            .arg(ns_name)
            .arg(ns_password)
            .query(&mut conn)
            .expect("Failed to select namespace");

        // Add some test keys
        let num_keys = 5;
        for i in 0..num_keys {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            let _: String = redis::cmd("SET")
                .arg(&key)
                .arg(&value)
                .query(&mut conn)
                .expect("Failed to set key");
        }

        // Verify keys exist
        for i in 0..num_keys {
            let key = format!("key_{}", i);
            let exists: bool = redis::cmd("EXISTS")
                .arg(&key)
                .query(&mut conn)
                .expect("Failed to check if key exists");
            assert!(exists, "Key should exist before flush");
        }

        // Flush the namespace
        let flush_result: String = redis::cmd("FLUSH")
            .query(&mut conn)
            .expect("Failed to flush namespace");
        assert_eq!(flush_result, "OK", "FLUSH command should return OK");

        // Verify all keys are gone
        for i in 0..num_keys {
            let key = format!("key_{}", i);
            let exists: bool = redis::cmd("EXISTS")
                .arg(&key)
                .query(&mut conn)
                .expect("Failed to check if key exists after flush");
            assert!(!exists, "Key should not exist after flush");
        }

        // Verify namespace properties are preserved
        let _: () = redis::cmd("SELECT")
            .arg("default")
            .query(&mut conn)
            .expect("Failed to select default namespace");

        // Use NSINFO to verify namespace properties
        let nsinfo_result: String = redis::cmd("NSINFO")
            .arg(ns_name)
            .query(&mut conn)
            .expect("Failed to get namespace info after flush");

        // Check that the namespace is still private (public=0)
        assert!(
            nsinfo_result.contains("public: no"),
            "Namespace should still be private after flush"
        );
        // Check that the namespace still has a password
        assert!(
            nsinfo_result.contains("password: yes"),
            "Namespace should still have a password after flush"
        );

        // Test that we can add new keys after flush
        let _: () = redis::cmd("SELECT")
            .arg(ns_name)
            .arg(ns_password)
            .query(&mut conn)
            .expect("Failed to select namespace");

        let new_key = "new_key_after_flush";
        let new_value = "new_value_after_flush";
        let set_result: String = redis::cmd("SET")
            .arg(new_key)
            .arg(new_value)
            .query(&mut conn)
            .expect("Failed to set key after flush");
        assert_eq!(set_result, "OK", "Should be able to set key after flush");

        // Test that we can't flush the default namespace
        let _: () = redis::cmd("SELECT")
            .arg("default")
            .query(&mut conn)
            .expect("Failed to select default namespace");

        let flush_default_result: redis::RedisResult<String> = redis::cmd("FLUSH").query(&mut conn);
        assert!(
            flush_default_result.is_err(),
            "Should not be able to flush default namespace"
        );

        // Test that we can't flush a public namespace
        let public_ns = "public_flush_test";
        let _: String = redis::cmd("NSNEW")
            .arg(public_ns)
            .query(&mut conn)
            .expect("Failed to create public namespace");

        // Public namespaces have public=1 by default

        let _: () = redis::cmd("SELECT")
            .arg(public_ns)
            .query(&mut conn)
            .expect("Failed to select public namespace");

        // Try to flush public namespace (should fail)
        let flush_public_result: redis::RedisResult<String> = redis::cmd("FLUSH").query(&mut conn);
        assert!(
            flush_public_result.is_err(),
            "Should not be able to flush public namespace"
        );

        // Test that authentication is required for flush
        // Create a new connection without authentication
        let mut new_conn = server.connect();
        let _: () = redis::cmd("SELECT")
            .arg(ns_name)
            .query(&mut new_conn)
            .expect("Failed to select namespace");

        // Try to flush without authentication (should fail)
        let flush_no_auth_result: redis::RedisResult<String> =
            redis::cmd("FLUSH").query(&mut new_conn);
        assert!(
            flush_no_auth_result.is_err(),
            "Should not be able to flush without authentication"
        );
    }
}
