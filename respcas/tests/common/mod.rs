//! One respcas daemon on a temporary store, shared by every integration
//! test file in this crate.

// Each test binary compiles this module separately, so anything only one of
// them uses is dead code in the others.
#![allow(dead_code)]

use cas_storage::config::DEFAULT_RESP_MAX_VALUE_SIZE;
use cas_storage::{BlockId, Object};
use redis::{Client, Connection};
use redis_protocol::resp2::types::OwnedFrame as Frame;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread::{self, sleep};
use std::time::{Duration, Instant};
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

/// Everything a test daemon is started with. `Default` is what
/// [`TestServer::new`] uses; a test that needs one thing different says only
/// that thing.
pub struct ServerConfig {
    pub admin_password: Option<String>,
    pub max_value_size: usize,
    pub options: cas_storage::StoreOptions,
    /// Serve this directory instead of a fresh temporary one -- how a test
    /// restarts a daemon on a store that already holds data.
    pub data_dir: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            admin_password: None,
            max_value_size: DEFAULT_RESP_MAX_VALUE_SIZE,
            options: store_options(),
            data_dir: None,
        }
    }
}

pub struct TestServer {
    port: u16,
    // Keep this field to ensure the directory isn't deleted
    _temp_dir: Option<tempfile::TempDir>,
    server_handle: Option<thread::JoinHandle<()>>,
    shutdown_sender: Option<oneshot::Sender<()>>,
    data_dir: PathBuf,
}

impl TestServer {
    pub fn new() -> Self {
        Self::new_with_admin(None)
    }

    pub fn new_with_admin(admin_password: Option<String>) -> Self {
        Self::with(ServerConfig {
            admin_password,
            ..ServerConfig::default()
        })
    }

    /// A daemon with the value cap set low, for the tests that check what
    /// happens at it (ADR 0014).
    pub fn new_with_max_value_size(max_value_size: usize) -> Self {
        Self::with(ServerConfig {
            max_value_size,
            ..ServerConfig::default()
        })
    }

    /// A daemon whose store keeps values of up to `data_bytes` in their own
    /// record and sends anything larger through the block write path (ADR
    /// 0014).
    ///
    /// The configured number is the size of the whole record, so the envelope
    /// the store always writes is added here -- a test says what it means by
    /// the threshold, which is how much VALUE fits inline.
    pub fn new_with_inline_threshold(data_bytes: usize) -> Self {
        Self::with(ServerConfig {
            options: cas_storage::StoreOptions {
                inline_metadata_size: Some(data_bytes + Object::minimum_inline_metadata_size()),
                ..store_options()
            },
            ..ServerConfig::default()
        })
    }

    /// A daemon on a store that already exists -- the second half of a
    /// restart.
    pub fn reopen(data_dir: &Path) -> Self {
        Self::with(ServerConfig {
            data_dir: Some(data_dir.to_path_buf()),
            ..ServerConfig::default()
        })
    }

    /// A daemon that acks nothing it has not fsynced (ADR 0013), which is what
    /// the binary's own default is. The other constructors leave the flush to
    /// the page cache, which is enough for a store that is thrown away but not
    /// for a test about what survives a shutdown.
    pub fn new_durable() -> Self {
        Self::with(ServerConfig {
            options: cas_storage::StoreOptions {
                durability: cas_storage::Durability::Fsync,
                ..store_options()
            },
            ..ServerConfig::default()
        })
    }

    pub fn with(config: ServerConfig) -> Self {
        let ServerConfig {
            admin_password,
            max_value_size,
            options,
            data_dir,
        } = config;

        // A caller-supplied directory belongs to the caller and outlives this
        // server; otherwise the store is a temporary of our own.
        let (temp_dir, data_dir) = match data_dir {
            Some(dir) => (None, dir),
            None => {
                let temp_dir = tempdir().expect("Failed to create temp directory");
                let dir = temp_dir.path().to_path_buf();
                (Some(temp_dir), dir)
            }
        };

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
                    respcas::storage::Storage::new(thread_data_dir, options)
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
            server_handle: Some(server_handle),
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

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Stops the daemon and waits for its store to be closed.
    ///
    /// Dropping is enough to stop serving, but only the join proves the store
    /// was released: fjall holds the directory lock until the last handle is
    /// dropped, and a test that reopens the store has to know that has
    /// happened rather than sleep on it.
    ///
    /// Takes `&mut self` on purpose: the temporary directory belongs to this
    /// value, so a test that stops a daemon to look at its store has to keep
    /// the daemon alive as a value, or the store it wanted to look at is
    /// deleted with it.
    pub fn stop(&mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        if let Some(sender) = self.shutdown_sender.take() {
            let _ = sender.send(());
        }
        if let Some(handle) = self.server_handle.take() {
            handle.join().expect("the server thread must not panic");
        }
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

    /// A connection this test drives byte by byte, for the assertions that
    /// are about the wire itself rather than about what a client library
    /// makes of it.
    pub fn raw(&self) -> RawConn {
        RawConn::connect(self.port)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        println!("Shutting down test server on port {}", self.port);
        self.shutdown();
    }
}

/// A socket with no client library on it: the test writes the bytes it means
/// and reads the frames that come back.
///
/// Everything here is blocking with a read timeout, so a server that never
/// answers fails the test instead of hanging the run.
pub struct RawConn {
    stream: std::net::TcpStream,
    buffer: Vec<u8>,
}

impl RawConn {
    pub fn connect(port: u16) -> Self {
        let stream = std::net::TcpStream::connect(("127.0.0.1", port))
            .expect("the daemon must accept a connection");
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .expect("a read timeout must be settable");
        // Every write is meant to reach the server as it was written; Nagle
        // would coalesce the fragments these tests send on purpose.
        stream.set_nodelay(true).expect("nodelay must be settable");
        Self {
            stream,
            buffer: Vec::new(),
        }
    }

    /// Writes bytes and flushes them.
    pub fn send(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).expect("the write must land");
        self.stream.flush().expect("the flush must work");
    }

    /// Writes a RESP array command: `*<n>\r\n$<len>\r\n<arg>\r\n...`.
    pub fn send_command(&mut self, args: &[&[u8]]) {
        self.send(&encode_command(args));
    }

    /// One reply frame, reading from the socket until it is complete.
    pub fn reply(&mut self) -> Frame {
        loop {
            match redis_protocol::resp2::decode::decode(&self.buffer) {
                Ok(Some((frame, len))) => {
                    self.buffer.drain(..len);
                    return frame;
                }
                Ok(None) => {}
                Err(e) => panic!("the daemon sent bytes that are not RESP: {e}"),
            }
            let mut chunk = [0u8; 64 * 1024];
            let read = self
                .stream
                .read(&mut chunk)
                .expect("the daemon must answer within the read timeout");
            if read == 0 {
                panic!(
                    "the daemon hung up with an incomplete reply in flight ({} bytes buffered)",
                    self.buffer.len()
                );
            }
            self.buffer.extend_from_slice(&chunk[..read]);
        }
    }

    /// `n` reply frames, in the order they arrive.
    pub fn replies(&mut self, n: usize) -> Vec<Frame> {
        (0..n).map(|_| self.reply()).collect()
    }

    /// Asserts the daemon has hung up, and that it sent nothing after the
    /// replies already read.
    ///
    /// A reset counts as a hangup: when the daemon closes a socket whose
    /// receive buffer still holds bytes it decided not to read -- the body of
    /// a refused oversized value, say -- the kernel answers the sender with
    /// RST rather than FIN, and the client sees ECONNRESET instead of EOF.
    /// Both mean the same thing here, which is that the connection is over.
    pub fn expect_hangup(&mut self) {
        let mut chunk = [0u8; 4096];
        match self.stream.read(&mut chunk) {
            Ok(0) => {}
            Ok(read) => panic!(
                "expected a hangup, got {} more bytes: {:?}",
                read,
                String::from_utf8_lossy(&chunk[..read])
            ),
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
            Err(e) => panic!("the daemon must close the connection, not go silent: {e}"),
        }
    }
}

/// `*<n>\r\n$<len>\r\n<arg>\r\n...`: one command as an array frame.
pub fn encode_command(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// The payload of a bulk string reply.
pub fn bulk(frame: &Frame) -> &[u8] {
    match frame {
        Frame::BulkString(bytes) => bytes,
        other => panic!("expected a bulk string, got {other:?}"),
    }
}

/// The text of a simple string reply.
pub fn simple(frame: &Frame) -> &str {
    match frame {
        Frame::SimpleString(bytes) => std::str::from_utf8(bytes).expect("a simple string is text"),
        other => panic!("expected a simple string, got {other:?}"),
    }
}

/// The value of an integer reply.
pub fn integer(frame: &Frame) -> i64 {
    match frame {
        Frame::Integer(value) => *value,
        other => panic!("expected an integer, got {other:?}"),
    }
}

/// The message of an error reply.
pub fn error(frame: &Frame) -> &str {
    match frame {
        Frame::Error(message) => message,
        other => panic!("expected an error, got {other:?}"),
    }
}

/// The elements of an array reply.
pub fn array(frame: &Frame) -> &[Frame] {
    match frame {
        Frame::Array(items) => items,
        other => panic!("expected an array, got {other:?}"),
    }
}

/// True for the nil reply GET answers a missing key with.
pub fn is_null(frame: &Frame) -> bool {
    matches!(frame, Frame::Null)
}

/// Opens the store at `dir` directly, for the assertions that are about what
/// is on disk rather than about what the wire said. The daemon must be
/// stopped first: fjall holds the directory lock.
pub fn open_store(dir: &Path) -> respcas::storage::Storage {
    respcas::storage::Storage::new(dir.to_path_buf(), store_options())
        .expect("the store must reopen")
}

/// The record `namespace` holds under `key`, decoded.
pub fn record(storage: &respcas::storage::Storage, namespace: &str, key: &[u8]) -> Option<Object> {
    let tree = storage
        .cas()
        .get_bucket(namespace)
        .expect("the namespace must exist");
    let raw = tree.get(key).expect("the lookup must work")?;
    Some(Object::try_from(&*raw).expect("a stored record must decode"))
}

/// How many references a block is carrying, or `None` if there is no such
/// block -- the exact rc every ADR 0008 assertion is made against.
pub fn rc_of(storage: &respcas::storage::Storage, block: &BlockId) -> Option<usize> {
    storage
        .cas()
        .block_tree()
        .expect("the block tree must open")
        .get_block(block.as_slice())
        .expect("the lookup must work")
        .map(|block| block.rc())
}

/// Every block record in the store.
pub fn block_count(storage: &respcas::storage::Storage) -> usize {
    storage
        .cas()
        .block_tree()
        .expect("the block tree must open")
        .iter_all()
        .count()
}

/// Every block DATA file under `blocks/`, so a test can say "no bytes were
/// written" rather than assume it.
///
/// The blocks database (`blocks/.db`) and the staging directory are dot-names,
/// and the header sidecar that database writes beside itself is named; a block
/// file is anything else, sitting in its fanout directory.
pub fn block_files(data_dir: &Path) -> usize {
    fn walk(dir: &Path, count: &mut usize) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy().to_string();
            if name.starts_with('.')
                || name == cas_storage::metastore::store_header::STORE_HEADER_SIDECAR
            {
                continue;
            }
            if path.is_dir() {
                walk(&path, count);
            } else {
                *count += 1;
            }
        }
    }

    let mut count = 0;
    walk(&data_dir.join("blocks"), &mut count);
    count
}

/// Rewinds the store at `root` to the shape respcas gave one before ADR 0014:
/// fjall's own files straight in the data directory, and no block store
/// anywhere.
///
/// The point is that such stores exist in the field. A test that wants to say
/// "this still works on a store from before the layout" has to build one, and
/// the only honest way to build one is to take a real store apart the way the
/// old code laid it out.
///
/// The daemon must be stopped and the store closed first.
pub fn rewind_to_pre_0014_layout(root: &Path) {
    for entry in fs::read_dir(root.join("db")).expect("the database directory must be readable") {
        let entry = entry.expect("the entry must be readable");
        fs::rename(entry.path(), root.join(entry.file_name())).expect("the move must work");
    }
    fs::remove_dir(root.join("db")).expect("the emptied directory must go");
    fs::remove_dir_all(root.join("blocks")).expect("the block store goes too");

    assert!(
        root.join("version").is_file(),
        "the pre-0014 shape: fjall's own marker in the data directory"
    );
}

/// The format version the store at `dir` says it is, read from the sidecar
/// copy of its header (ADR 0002; raised to 4 by ADR 0014).
pub fn header_version(dir: &Path) -> u16 {
    let raw = fs::read(dir.join("store_header.bin")).expect("the header sidecar must be there");
    cas_storage::StoreHeader::from_bytes(&raw)
        .expect("the header must decode")
        .version()
}

/// The real `respcas` binary, running as a child process.
///
/// The in-process server is a thread and cannot be killed without taking the
/// test with it; a daemon that has to survive being killed has to be a
/// process. Started on the defaults the binary itself resolves, which is
/// where its fsync durability comes from.
pub struct ChildServer {
    child: Child,
    port: u16,
    data_dir: PathBuf,
}

impl ChildServer {
    /// Spawns a daemon on `data_dir` and waits until it is serving.
    pub fn spawn(data_dir: &Path) -> Self {
        Self::spawn_with_config(data_dir, "")
    }

    /// Spawns a daemon whose `qss_storage.toml` is `config`, so a test can
    /// assert on a knob that has no flag -- the only way such a knob can be
    /// shown to reach the wire at all.
    ///
    /// The `[resp] data_dir`, `host` and `port` keys are still overridden by
    /// flags, because the test picks those.
    pub fn spawn_with_config(data_dir: &Path, config: &str) -> Self {
        fs::create_dir_all(data_dir).expect("the data directory must be creatable");

        // An OS-assigned port, released before the child is told to take it.
        // Nothing else in the test run is listening on 127.0.0.1 with an
        // ephemeral port it did not itself just get.
        let port = {
            let probe = TcpListener::bind("127.0.0.1:0").expect("a port must be assignable");
            probe
                .local_addr()
                .expect("the port must be readable")
                .port()
        };

        // Always a config file of our own, so the daemon cannot pick up a
        // qss_storage.toml from whatever directory the test run started in.
        let config_path = data_dir.join("qss_storage.toml");
        fs::write(&config_path, config).expect("the config file must be writable");

        let child = Command::new(env!("CARGO_BIN_EXE_respcas"))
            .arg("--config")
            .arg(&config_path)
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("the respcas binary must start");

        let mut server = Self {
            child,
            port,
            data_dir: data_dir.to_path_buf(),
        };
        server.wait_until_serving();
        server
    }

    /// Connects, retrying while the daemon is still coming up. Bounded: a
    /// daemon that never serves, or one that exits, fails the test.
    fn wait_until_serving(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait().expect("the child must be waitable") {
                panic!("the daemon exited before it served anything: {status}");
            }
            if std::net::TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the daemon never started serving on port {}",
                self.port
            );
            sleep(Duration::from_millis(20));
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn connect(&self) -> Connection {
        let url = format!("redis://127.0.0.1:{}", self.port);
        Client::open(url.as_str())
            .expect("Failed to create Redis client")
            .get_connection()
            .expect("the daemon must accept a connection")
    }

    /// Kills the daemon outright -- no shutdown, no close, nothing flushed
    /// that was not already durable -- and waits for it to be gone.
    pub fn kill(mut self) {
        self.child.kill().expect("the daemon must be killable");
        self.child.wait().expect("the daemon must be reapable");
    }
}

impl Drop for ChildServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
