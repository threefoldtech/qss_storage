use anyhow::Result;
use bytes::BytesMut;
use redis_protocol::resp2::types::OwnedFrame as Frame;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::cmd::{Command, CommandHandler};
use crate::conn::Conn;
use crate::namespace::NamespaceCache;
use crate::resp::{self, RespHelper};
use crate::storage::Storage;

/// Stays `pub` although no test calls it: the lib's copy of this module has no
/// caller for it (main.rs compiles its own), so `pub(crate)` would make it dead
/// code.
pub async fn run(
    addr: String,
    storage: Storage,
    admin_password: Option<String>,
    max_value_size: usize,
) -> Result<()> {
    // Initialize the default namespace if it doesn't exist
    if let Err(e) = storage.init_namespace() {
        error!("Failed to initialize namespace: {}", e);
        return Err(anyhow::anyhow!("Failed to initialize namespace: {}", e));
    }

    // Create a TCP listener
    let listener = TcpListener::bind(&addr).await?;
    info!("Listening on: {}", addr);

    // Create a shared storage instance
    let storage = Arc::new(storage);

    // Create a shared namespace cache
    let namespace_cache = Arc::new(NamespaceCache::new(storage.clone()));

    // Accept connections and process them
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                info!("Accepted connection from: {}", addr);

                // Clone the storage for this connection
                let storage = storage.clone();

                // Clone the namespace cache for this connection
                let namespace_cache = namespace_cache.clone();

                // Clone the admin password for this connection
                let admin_password = admin_password.clone();

                // Spawn a new task to handle this connection
                tokio::spawn(async move {
                    if let Err(e) = process(
                        socket,
                        storage,
                        namespace_cache.clone(),
                        admin_password,
                        max_value_size,
                    )
                    .await
                    {
                        error!("Error processing connection: {}", e);
                    }
                });
            }
            Err(e) => {
                error!("Error accepting connection: {}", e);
            }
        }
    }
}

pub async fn process(
    socket: TcpStream,
    storage: Arc<Storage>,
    namespace_cache: Arc<NamespaceCache>,
    admin_password: Option<String>,
    max_value_size: usize,
) -> Result<()> {
    Session::new(
        socket,
        storage,
        namespace_cache,
        admin_password,
        max_value_size,
    )?
    .run()
    .await
}

/// Why a session stopped serving a connection.
enum SessionEnd {
    /// The client can no longer be written to.
    WriteFailed,
    /// The client sent bytes that are not a command.
    ProtocolError,
}

/// Everything one client connection owns: the socket, the handler bound to the
/// connection's current namespace, and the shared state needed to rebind that
/// handler when SELECT or AUTH changes the connection's identity.
struct Session {
    conn: Conn,
    handler: CommandHandler,
    storage: Arc<Storage>,
    namespace_cache: Arc<NamespaceCache>,
    admin_password: Option<String>,
    /// Largest value this connection may send (ADR 0014). A command that
    /// declares a longer one is refused before its bytes are read.
    max_value_size: usize,
}

impl Session {
    fn new(
        socket: TcpStream,
        storage: Arc<Storage>,
        namespace_cache: Arc<NamespaceCache>,
        admin_password: Option<String>,
        max_value_size: usize,
    ) -> Result<Self> {
        // If no admin password is required, all connections are admin by default
        let is_admin = admin_password.is_none();
        let conn = Conn::new(socket, is_admin);

        // Try to get or create a namespace for the default namespace using the cache
        let namespace = match namespace_cache.create_if_not_exists(conn.get_namespace()) {
            Ok(namespace) => namespace,
            Err(e) => {
                error!("Failed to initialize default namespace: {}", e);
                return Err(anyhow::anyhow!(
                    "Failed to initialize default namespace: {}",
                    e
                ));
            }
        };

        // Create a command handler with the connection's namespace, namespace cache, and admin status
        let handler = CommandHandler::new(
            storage.clone(),
            namespace,
            namespace_cache.clone(),
            conn.is_admin(),
        );

        Ok(Self {
            conn,
            handler,
            storage,
            namespace_cache,
            admin_password,
            max_value_size,
        })
    }

    /// Read from the socket until the client goes away, answering every
    /// complete frame that arrives.
    async fn run(mut self) -> Result<()> {
        // Use BytesMut for zero-copy operations
        let mut buffer = BytesMut::with_capacity(4096);

        loop {
            // Read data directly into BytesMut buffer
            // This avoids an extra copy compared to using Vec<u8>
            match self.conn.read_buf(&mut buffer).await {
                Ok(0) => break, // Connection closed
                Ok(_) => {}
                Err(e) => {
                    error!("Error reading from socket: {}", e);
                    break;
                }
            }

            // Before the bytes are read: a command that declares a value
            // over the cap is refused now, while the buffer holds only its
            // header (ADR 0014). Answering after buffering it would have
            // paid the memory the cap exists to bound.
            if let Some(declared) = resp::oversized_bulk(&buffer, self.max_value_size) {
                warn!(
                    "refusing a {declared} byte argument: over the {} byte limit",
                    self.max_value_size
                );
                let _ = self
                    .write_response(&Frame::Error(format!(
                        "ERR value of {declared} bytes is over the {} byte limit \
                         (resp.max_value_size)",
                        self.max_value_size
                    )))
                    .await;
                // A stream has no resynchronisation point: the bytes that
                // were refused are still on their way, and everything after
                // them would be read as commands.
                break;
            }

            let consumed = match self.serve_buffered_frames(&buffer).await {
                Ok(consumed) => consumed,
                // Either the client is unreachable (encode or socket write
                // failed, and the error is already logged) so nothing further
                // can be delivered, or it sent bytes RESP cannot resume from.
                // Both end the connection rather than reading on.
                Err(SessionEnd::WriteFailed | SessionEnd::ProtocolError) => break,
            };

            // Remove processed data using split_to which is zero-copy
            if consumed > 0 {
                let _ = buffer.split_to(consumed); // Ignore the return value as suggested by the compiler
            }
        }

        debug!("Client disconnected");
        Ok(())
    }

    /// Answer every complete frame sitting in `buffer`, returning how many
    /// bytes were consumed. Stops early on a partial frame, which is simply
    /// waiting for more of itself; a malformed frame and a failed write are
    /// both fatal for the session and returned as an error so the caller
    /// drops the connection.
    async fn serve_buffered_frames(&mut self, buffer: &[u8]) -> Result<usize, SessionEnd> {
        let mut pos = 0;

        while pos < buffer.len() {
            // Create a slice starting at the current position
            match RespHelper::parse_frame(&buffer[pos..]) {
                Ok((Some(frame), len)) => {
                    debug!("Received frame: {:?}", frame);
                    pos += len;

                    let response = self.dispatch(frame).await;
                    self.write_response(&response).await?;
                }
                // Need more data. Anything consumed on the way was blank
                // lines, which are dropped rather than kept waiting for a
                // command to follow them.
                Ok((None, skipped)) => {
                    pos += skipped;
                    break;
                }
                Err(e) => {
                    // A client sending bytes that are not RESP is the
                    // client's problem, connection-scoped and answered on
                    // that connection. ERROR is reserved for the daemon
                    // itself being in trouble -- it is what operators (and
                    // the campaign's daemon-log gate) alert on.
                    warn!("Error parsing frame: {}", e);

                    // A stream has no resynchronisation point: whatever
                    // follows a malformed frame cannot be parsed either. Say
                    // so and hang up, as Redis does. Reading on would answer
                    // nothing while the unparseable bytes accumulated.
                    // `RespError::Protocol` already reads "Protocol error: ..."
                    self.write_response(&Frame::Error(format!("ERR {}", e)))
                        .await?;
                    return Err(SessionEnd::ProtocolError);
                }
            }
        }

        Ok(pos)
    }

    /// Route one frame to its handler.
    ///
    /// SELECT and AUTH are special: they rebind this session's handler, so they
    /// are served here rather than by `CommandHandler`.
    async fn dispatch(&mut self, frame: Frame) -> Frame {
        match Command::from_frame(frame) {
            Ok(Command::Select {
                namespace,
                password,
            }) => self.select_namespace(namespace, password),
            Ok(Command::Auth { password }) => self.authenticate(password),
            Ok(cmd) => self.handler.execute(cmd).await,
            Err(e) => {
                // Unknown command or wrong arity: the client's mistake,
                // reported to the client. Not an ERROR -- see the frame
                // parser above.
                warn!("Error parsing command: {}", e);
                Frame::Error(format!("Error: {}", e))
            }
        }
    }

    /// SELECT: switch the connection to another namespace, rebinding the
    /// handler with the authentication status implied by the password given.
    fn select_namespace(&mut self, namespace: String, password: Option<String>) -> Frame {
        debug!("Handling SELECT command for namespace: {}", namespace);

        // Switch the connection's namespace
        self.conn.set_namespace(namespace.clone());

        // Get or create the namespace from the cache
        match self.namespace_cache.get_or_create(namespace.clone()) {
            Ok(namespace_obj) => {
                let is_authenticated =
                    self.namespace_password_accepted(&namespace, password.as_deref());

                // Replace the handler with one bound to the new namespace
                let mut new_handler = CommandHandler::new(
                    self.storage.clone(),
                    namespace_obj,
                    self.namespace_cache.clone(),
                    self.conn.is_admin(),
                );
                new_handler.set_namespace_authenticated(is_authenticated);
                self.handler = new_handler;

                if is_authenticated {
                    Frame::SimpleString("OK".into())
                } else {
                    // Access is still granted, but writes will be refused
                    Frame::SimpleString("OK (read-only access)".into())
                }
            }
            Err(e) => {
                error!("Error selecting namespace: {}", e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// True when the namespace has no password, or `provided` matches it.
    /// A namespace whose metadata cannot be read is treated as not authenticated.
    fn namespace_password_accepted(&self, namespace: &str, provided: Option<&str>) -> bool {
        match self.storage.get_namespace_meta(namespace) {
            Ok(meta) => match &meta.password {
                Some(ns_password) => provided == Some(ns_password.as_str()),
                None => true,
            },
            Err(e) => {
                error!("Error getting namespace metadata: {}", e);
                false
            }
        }
    }

    /// AUTH: verify the admin password and, on success, rebind the handler so
    /// it sees the connection's new admin status.
    fn authenticate(&mut self, password: String) -> Frame {
        debug!("Handling AUTH command");

        let admin_password = match &self.admin_password {
            Some(admin_password) => admin_password,
            // No admin password set, all connections are already admin
            None => return Frame::SimpleString("OK".into()),
        };

        if password != *admin_password {
            return Frame::Error("ERR invalid password".into());
        }

        self.conn.set_admin(true);

        // Rebuild the handler from the connection's current namespace so the
        // new admin status takes effect
        match self
            .namespace_cache
            .get_or_create(self.conn.get_namespace())
        {
            Ok(namespace) => {
                self.handler = CommandHandler::new(
                    self.storage.clone(),
                    namespace,
                    self.namespace_cache.clone(),
                    self.conn.is_admin(),
                );
                Frame::SimpleString("OK".into())
            }
            Err(e) => {
                error!("Error recreating namespace after AUTH: {}", e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Encode a reply and push it to the client.
    async fn write_response(&mut self, response: &Frame) -> Result<(), SessionEnd> {
        let bytes = match RespHelper::encode_frame(response) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Error encoding response: {}", e);
                return Err(SessionEnd::WriteFailed);
            }
        };

        if let Err(e) = self.conn.write_all(&bytes).await {
            error!("Error writing response: {}", e);
            return Err(SessionEnd::WriteFailed);
        }

        Ok(())
    }
}
