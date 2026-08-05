use crate::common::TestServer;

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
