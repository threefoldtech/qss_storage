use crate::common::TestServer;

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
