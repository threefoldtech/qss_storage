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
                let storage = Arc::new(respd::storage::Storage::new(thread_data_dir, None));

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

        Self {
            port,
            _temp_dir: temp_dir,
            _server_handle: server_handle,
            shutdown_sender: Some(shutdown_sender),
        }
    }

    fn connect(&self) -> Connection {
        let client = Client::open(format!("redis://127.0.0.1:{}/", self.port))
            .expect("Failed to create Redis client");

        // Try to connect a few times, as the server might not be ready yet
        for _ in 0..10 {
            match client.get_connection() {
                Ok(conn) => return conn,
                Err(_) => sleep(Duration::from_millis(100)),
            }
        }

        panic!("Failed to connect to Redis server after multiple attempts");
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Send shutdown signal if the server is still running
        if let Some(sender) = self.shutdown_sender.take() {
            let _ = sender.send(());
        }
    }
}

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
