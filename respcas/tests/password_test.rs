//! Namespace and admin authentication, from the wire.

mod common;

use common::TestServer;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_authentication() {
        // Create a server
        let server = TestServer::new();
        let mut conn = server.connect();

        // Create a namespace
        let ns_name = "test_auth_ns";

        // Create the namespace
        let create_result: String = redis::cmd("NSNEW")
            .arg(ns_name)
            .query(&mut conn)
            .expect("Failed to create namespace");
        assert_eq!(create_result, "OK");

        // Switch to the namespace
        let select_result: String = redis::cmd("SELECT")
            .arg(ns_name)
            .query(&mut conn)
            .expect("Failed to select namespace");
        assert_eq!(select_result, "OK");

        // Set a key in the namespace
        let set_result: String = redis::cmd("SET")
            .arg("test_key")
            .arg("test_value")
            .query(&mut conn)
            .expect("Failed to set key");
        assert_eq!(set_result, "OK");

        // Delete a key in the namespace
        let del_result: i32 = redis::cmd("DEL")
            .arg("test_key")
            .query(&mut conn)
            .expect("Failed to delete key");
        assert_eq!(del_result, 1);

        // Set the namespace to WORM mode
        let nsset_result: String = redis::cmd("NSSET")
            .arg(ns_name)
            .arg("worm")
            .arg("1")
            .query(&mut conn)
            .expect("Failed to set namespace to WORM mode");
        assert_eq!(nsset_result, "OK");

        // Set a key in WORM mode
        let set_result: String = redis::cmd("SET")
            .arg("worm_key")
            .arg("initial_value")
            .query(&mut conn)
            .expect("Failed to set key in WORM mode");
        assert_eq!(set_result, "OK");

        // Try to modify the key in WORM mode (should fail)
        let set_result = redis::cmd("SET")
            .arg("worm_key")
            .arg("modified_value")
            .query::<String>(&mut conn);
        assert!(
            set_result.is_err(),
            "Should not be able to modify key in WORM mode"
        );

        // Try to delete the key in WORM mode (should fail)
        let del_result = redis::cmd("DEL").arg("worm_key").query::<i32>(&mut conn);
        assert!(
            del_result.is_err(),
            "Should not be able to delete key in WORM mode"
        );
    }

    #[test]
    fn test_command_handler_authentication() {
        // Create a server with admin password
        let server = TestServer::new_with_admin(Some("admin123".to_string()));
        let mut conn = server.connect();

        // First, authenticate as admin
        let auth_result: String = redis::cmd("AUTH")
            .arg("admin123")
            .query(&mut conn)
            .expect("Failed to authenticate as admin");
        assert_eq!(auth_result, "OK");

        // Create a namespace
        let ns_name = "auth_test_ns";
        let create_result: String = redis::cmd("NSNEW")
            .arg(ns_name)
            .query(&mut conn)
            .expect("Failed to create namespace");
        assert_eq!(create_result, "OK");

        // Switch to the namespace
        let select_result: String = redis::cmd("SELECT")
            .arg(ns_name)
            .query(&mut conn)
            .expect("Failed to select namespace");
        assert_eq!(select_result, "OK");

        // Set a key
        let set_result: String = redis::cmd("SET")
            .arg("auth_key")
            .arg("auth_value")
            .query(&mut conn)
            .expect("Failed to set key");
        assert_eq!(set_result, "OK");

        // Read the key
        let get_result: String = redis::cmd("GET")
            .arg("auth_key")
            .query(&mut conn)
            .expect("Failed to get key");
        assert_eq!(get_result, "auth_value");

        // Delete the key
        let del_result: i32 = redis::cmd("DEL")
            .arg("auth_key")
            .query(&mut conn)
            .expect("Failed to delete key");
        assert_eq!(del_result, 1);
    }
}
