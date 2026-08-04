//! One respcas daemon on a temporary store, shared by every integration
//! test file in this crate.

// Each test binary compiles this module separately, so anything only one of
// them uses is dead code in the others.
#![allow(dead_code)]

use cas_storage::config::DEFAULT_RESP_MAX_VALUE_SIZE;
use redis::{Client, Connection};
use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::{self, sleep};
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::oneshot;

/// How a test daemon opens its store: everything inlined that fits, which is
/// respcas's own default, and the flush left to the page cache -- these
/// stores are thrown away, and none of these tests is a crash test.
pub fn store_options() -> cas_storage::StoreOptions {
    cas_storage::StoreOptions {
        inline_metadata_size: Some(cas_storage::config::DEFAULT_RESP_INLINE_METADATA_SIZE),
        durability: cas_storage::Durability::Buffer,
        ..cas_storage::StoreOptions::default()
    }
}

pub struct TestServer {
    port: u16,
    _temp_dir: tempfile::TempDir, // Keep this field to ensure the directory isn't deleted
    _server_handle: thread::JoinHandle<()>,
    shutdown_sender: Option<oneshot::Sender<()>>,
    data_dir: PathBuf,
}

impl TestServer {
    pub fn new() -> Self {
        Self::new_with_admin(None)
    }

    pub fn new_with_admin(admin_password: Option<String>) -> Self {
        Self::start(admin_password, DEFAULT_RESP_MAX_VALUE_SIZE)
    }

    /// A daemon with the value cap set low, for the tests that check what
    /// happens at it (ADR 0014).
    pub fn new_with_max_value_size(max_value_size: usize) -> Self {
        Self::start(None, max_value_size)
    }

    fn start(admin_password: Option<String>, max_value_size: usize) -> Self {
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
        println!("Starting respcas server on port {}", port);

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
                    respcas::storage::Storage::new(thread_data_dir, store_options())
                        .expect("can open storage"),
                );

                // Create a shared namespace cache
                let namespace_cache = Arc::new(respcas::namespace::NamespaceCache::new(storage.clone()));

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
                                        if let Err(e) = respcas::server::process(socket, storage, namespace_cache, admin_password, max_value_size).await {
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
            data_dir,
        }
    }

    /// The store this daemon serves. A test that wants to look at the store
    /// itself -- fsck, the on-disk layout -- must stop the daemon first:
    /// fjall holds a directory lock.
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    pub fn connect(&self) -> Connection {
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
