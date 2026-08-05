use crate::common::TestServer;

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
    let flush_no_auth_result: redis::RedisResult<String> = redis::cmd("FLUSH").query(&mut new_conn);
    assert!(
        flush_no_auth_result.is_err(),
        "Should not be able to flush without authentication"
    );
}
