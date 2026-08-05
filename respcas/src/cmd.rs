use bytes::Bytes;
use redis_protocol::resp2::types::OwnedFrame as Frame;
use std::sync::Arc;
use thiserror::Error;
use tracing::debug;

use crate::content;
use crate::namespace::{Namespace, NamespaceCache};
use crate::storage::Storage;

mod keys;
mod ns;
mod parse;

/// Keys one SCAN or RSCAN answers with. The cursor of a full page is its
/// last key; a shorter page ends the scan.
const SCAN_PAGE: usize = 10;

/// A key as a log line should show it: itself when it is text, hex when it
/// is a hash (ADR 0014). Never a lossy decode, which would print the same
/// replacement characters for two different keys.
fn shown(key: &[u8]) -> String {
    match std::str::from_utf8(key) {
        Ok(text) => text.to_string(),
        Err(_) => content::hex(key),
    }
}

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("Unknown command: {0}")]
    UnknownCommand(String),

    #[error("Wrong number of arguments for command: {0}")]
    WrongNumberOfArguments(String),

    #[error("Storage error: {0}")]
    Storage(#[from] cas_storage::MetaError),

    #[error("Protocol error: {0}")]
    Protocol(String),
}

/// Redis command types supported by our server
///
/// Keys are `Bytes`, not `String`: a content-addressed namespace keys its
/// records by the raw BLAKE3 of the value (ADR 0014), which is binary, and a
/// lossy UTF-8 decode on the way in would quietly address a different record
/// than the client named. RESP is binary-safe end to end, so nothing else
/// had to change for that to work.
#[derive(Debug)]
pub enum Command {
    Get {
        key: Bytes,
    },
    MGet {
        keys: Vec<Bytes>,
    },
    Set {
        key: Bytes,
        value: Bytes,
    },
    /// `CSET <value>`: the content-addressed put, valid only in a cas
    /// namespace. Equivalent to `SET "" <value>`, and the reply is the same
    /// -- the 32-byte key the server computed (ADR 0014).
    CSet {
        value: Bytes,
    },
    Ping {
        message: Option<String>,
    },
    Echo {
        message: Bytes,
    },
    Del {
        keys: Vec<Bytes>,
    },
    Exists {
        key: Bytes,
    },
    Check {
        key: Bytes,
    },
    Length {
        key: Bytes,
    },
    KeyTime {
        key: Bytes,
    },
    Select {
        namespace: String,
        password: Option<String>,
    },
    NSNew {
        name: String,
    },
    NSInfo {
        name: String,
    },
    NSList,
    Auth {
        password: String,
    },
    DBSize,
    Scan {
        cursor: Option<Bytes>,
    },
    RScan {
        cursor: Option<Bytes>,
    },
    NSSet {
        namespace: String,
        property: String,
        value: String,
    },
    Flush,
    Time,
    // Add more commands as needed
}

/// Handler for Redis commands
pub(crate) struct CommandHandler {
    storage: Arc<Storage>,

    // Namespace for this connection
    namespace: Arc<Namespace>,

    // Namespace cache shared across all connections
    namespace_cache: Arc<NamespaceCache>,

    // Flag indicating if this connection has admin privileges
    is_admin: bool,

    // Flag indicating if this connection is authenticated for the current namespace
    // This is set to true if the namespace has no password or if the correct password was provided
    namespace_authenticated: bool,
}

impl CommandHandler {
    /// Create a new command handler
    pub(crate) fn new(
        storage: Arc<Storage>,
        namespace: Arc<Namespace>,
        namespace_cache: Arc<NamespaceCache>,
        is_admin: bool,
    ) -> Self {
        // Check if namespace has a password by querying its metadata
        let namespace_authenticated = match storage
            .get_namespace_meta(&namespace.properties.read().unwrap().namespace_name)
        {
            Ok(meta) => meta.password.is_none(),
            Err(_) => true, // Default to authenticated if we can't get metadata
        };

        Self {
            storage,
            namespace,
            namespace_cache,
            is_admin,
            namespace_authenticated,
        }
    }

    /// Set the authentication status for the current namespace
    pub(crate) fn set_namespace_authenticated(&mut self, authenticated: bool) {
        self.namespace_authenticated = authenticated;
        debug!("Set namespace authentication status to {}", authenticated);
    }

    /// Whether this connection is authenticated for the namespace it is on.
    ///
    /// Read by the session when it rebuilds the handler for a reason that is
    /// not a namespace change -- AUTH -- so that what a `SELECT <ns> <password>`
    /// granted survives the rebuild.
    pub(crate) fn namespace_authenticated(&self) -> bool {
        self.namespace_authenticated
    }
    /// Execute a command and return the response frame
    ///
    /// Async since ADR 0014: the storage paths a command can reach are the
    /// block engine's, and those are async all the way down (they take
    /// stripes and run their disk work on blocking threads). Nothing here
    /// spawns or waits on anything else.
    pub(crate) async fn execute(&self, cmd: Command) -> Frame {
        match cmd {
            Command::Get { key } => self.handle_get(&key).await,
            Command::MGet { keys } => self.handle_mget(keys).await,
            Command::Set { key, value } => self.handle_set(key, value).await,
            Command::CSet { value } => self.handle_cset(value).await,
            Command::Ping { message } => Self::handle_ping(message),
            Command::Echo { message } => Self::handle_echo(message),
            Command::Del { keys } => self.handle_del(keys).await,
            Command::Exists { key } => self.handle_exists(&key),
            Command::Check { key } => self.handle_check(&key).await,
            Command::Length { key } => self.handle_length(&key),
            Command::KeyTime { key } => self.handle_keytime(&key),
            Command::NSNew { name } => self.handle_nsnew(name),
            Command::NSInfo { name } => self.handle_nsinfo(name),
            Command::NSList => self.handle_nslist(),
            Command::DBSize => self.handle_dbsize(),
            Command::Scan { cursor } => self.handle_scan(cursor),
            Command::RScan { cursor } => self.handle_rscan(cursor),
            Command::NSSet {
                namespace,
                property,
                value,
            } => self.handle_nsset(namespace, property, value),
            Command::Flush => self.handle_flush(),
            Command::Time => Self::handle_time(),
            Command::Select { .. } => {
                // SELECT is handled at a higher level in the connection handler
                Frame::Error("ERR SELECT should be handled at connection level".into())
            }
            Command::Auth { .. } => {
                // AUTH is handled at a higher level in the connection handler
                Frame::Error("ERR AUTH should be handled at connection level".into())
            }
        }
    }

    /// Whether this connection may read here.
    fn may_read(&self) -> bool {
        let props = self.namespace.properties.read().unwrap();
        props.public || self.namespace_authenticated
    }
}
