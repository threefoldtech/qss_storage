use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Connection abstraction for client connections
pub struct Conn {
    /// The TCP socket for the connection
    socket: TcpStream,
    /// Namespace for the connection
    namespace: String,
    /// Flag indicating if this connection has admin privileges
    admin: bool,
}

impl Conn {
    /// Create a new connection
    pub fn new(socket: TcpStream, is_admin: bool) -> Self {
        Self {
            socket,
            namespace: "default".to_string(),
            admin: is_admin,
        }
    }

    /// Set the namespace for this connection
    pub fn set_namespace(&mut self, namespace: String) {
        self.namespace = namespace;
    }

    /// Get the current namespace
    pub fn get_namespace(&self) -> String {
        self.namespace.clone()
    }

    /// Read data from the socket into the provided buffer
    pub async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<usize> {
        self.socket.read_buf(buf).await
    }

    /// Check if the connection has admin privileges
    pub fn is_admin(&self) -> bool {
        self.admin
    }

    /// Set admin status for this connection
    pub fn set_admin(&mut self, admin: bool) {
        self.admin = admin;
    }

    /// Write data to the socket
    pub async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.socket.write_all(buf).await
    }
}
