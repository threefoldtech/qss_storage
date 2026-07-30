use bytes::Bytes;
use redis_protocol::resp2::types::OwnedFrame as Frame;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error};

use crate::namespace::{Namespace, NamespaceCache};
use crate::property::BoolPropertyValue;
use crate::storage::Storage;

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
#[derive(Debug)]
pub enum Command {
    Get {
        key: String,
    },
    MGet {
        keys: Vec<String>,
    },
    Set {
        key: String,
        value: Bytes,
    },
    Ping {
        message: Option<String>,
    },
    Del {
        key: String,
    },
    Exists {
        key: String,
    },
    Check {
        key: String,
    },
    Length {
        key: String,
    },
    KeyTime {
        key: String,
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
        cursor: Option<String>,
    },
    RScan {
        cursor: Option<String>,
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

impl Command {
    /// Parse a Redis protocol frame into a command
    pub fn from_frame(frame: Frame) -> Result<Self, CommandError> {
        match frame {
            Frame::Array(array) => {
                if array.is_empty() {
                    return Err(CommandError::WrongNumberOfArguments(
                        "empty command".to_string(),
                    ));
                }

                // Extract the command name from the first element
                let command_name = match &array[0] {
                    Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_uppercase(),
                    _ => {
                        return Err(CommandError::Protocol(
                            "Command name must be a bulk string".to_string(),
                        ))
                    }
                };

                // Parse the command based on its name
                match command_name.as_str() {
                    "MGET" => {
                        if array.len() < 2 {
                            return Err(CommandError::WrongNumberOfArguments("MGET".to_string()));
                        }

                        let mut keys = Vec::with_capacity(array.len() - 1);
                        for item in array.iter().skip(1) {
                            let key = match item {
                                Frame::BulkString(bytes) => {
                                    String::from_utf8_lossy(bytes).to_string()
                                }
                                _ => {
                                    return Err(CommandError::Protocol(
                                        "MGET key must be a bulk string".to_string(),
                                    ))
                                }
                            };
                            keys.push(key);
                        }

                        Ok(Command::MGet { keys })
                    }
                    "SELECT" => {
                        // SELECT command should accept 2 or 3 arguments
                        // 2 args: SELECT namespace
                        // 3 args: SELECT namespace password
                        if array.len() < 2 || array.len() > 3 {
                            return Err(CommandError::WrongNumberOfArguments("SELECT".to_string()));
                        }

                        let namespace = match &array[1] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "SELECT namespace must be a bulk string".to_string(),
                                ))
                            }
                        };

                        // Parse optional password if provided
                        let password = if array.len() == 3 {
                            match &array[2] {
                                Frame::BulkString(bytes) => {
                                    Some(String::from_utf8_lossy(bytes).to_string())
                                }
                                _ => {
                                    return Err(CommandError::Protocol(
                                        "SELECT password must be a bulk string".to_string(),
                                    ))
                                }
                            }
                        } else {
                            None
                        };

                        Ok(Command::Select {
                            namespace,
                            password,
                        })
                    }
                    "NSNEW" => {
                        if array.len() != 2 {
                            return Err(CommandError::WrongNumberOfArguments("NSNEW".to_string()));
                        }

                        let name = match &array[1] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "NSNEW name must be a bulk string".to_string(),
                                ))
                            }
                        };

                        Ok(Command::NSNew { name })
                    }
                    "NSINFO" => {
                        if array.len() != 2 {
                            return Err(CommandError::WrongNumberOfArguments("NSINFO".to_string()));
                        }

                        let name = match &array[1] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "NSINFO name must be a bulk string".to_string(),
                                ))
                            }
                        };

                        Ok(Command::NSInfo { name })
                    }
                    "NSLIST" => {
                        if array.len() != 1 {
                            return Err(CommandError::WrongNumberOfArguments("NSLIST".to_string()));
                        }

                        Ok(Command::NSList)
                    }
                    "NSSET" => {
                        if array.len() != 4 {
                            return Err(CommandError::WrongNumberOfArguments("NSSET".to_string()));
                        }

                        let namespace = match &array[1] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "NSSET namespace must be a bulk string".to_string(),
                                ))
                            }
                        };

                        let property = match &array[2] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "NSSET property must be a bulk string".to_string(),
                                ))
                            }
                        };

                        let value = match &array[3] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "NSSET value must be a bulk string".to_string(),
                                ))
                            }
                        };

                        Ok(Command::NSSet {
                            namespace,
                            property,
                            value,
                        })
                    }
                    "DEL" => {
                        if array.len() != 2 {
                            return Err(CommandError::WrongNumberOfArguments("DEL".to_string()));
                        }

                        let key = match &array[1] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "DEL key must be a bulk string".to_string(),
                                ))
                            }
                        };

                        Ok(Command::Del { key })
                    }
                    "EXISTS" => {
                        if array.len() != 2 {
                            return Err(CommandError::WrongNumberOfArguments("EXISTS".to_string()));
                        }

                        let key = match &array[1] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "EXISTS key must be a bulk string".to_string(),
                                ))
                            }
                        };

                        Ok(Command::Exists { key })
                    }
                    "CHECK" => {
                        if array.len() != 2 {
                            return Err(CommandError::WrongNumberOfArguments("CHECK".to_string()));
                        }

                        let key = match &array[1] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "CHECK key must be a bulk string".to_string(),
                                ))
                            }
                        };

                        Ok(Command::Check { key })
                    }
                    "GET" => {
                        if array.len() != 2 {
                            return Err(CommandError::WrongNumberOfArguments("GET".to_string()));
                        }
                        let key = match &array[1] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "GET key must be a bulk string".to_string(),
                                ))
                            }
                        };
                        Ok(Command::Get { key })
                    }
                    "SET" => {
                        if array.len() < 3 {
                            return Err(CommandError::WrongNumberOfArguments("SET".to_string()));
                        }
                        let key = match &array[1] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "SET key must be a bulk string".to_string(),
                                ))
                            }
                        };
                        let value = match &array[2] {
                            Frame::BulkString(bytes) => Bytes::from(bytes.clone()),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "SET value must be a bulk string".to_string(),
                                ))
                            }
                        };
                        Ok(Command::Set { key, value })
                    }
                    "PING" => {
                        let message = if array.len() > 1 {
                            match &array[1] {
                                Frame::BulkString(bytes) => {
                                    Some(String::from_utf8_lossy(bytes).to_string())
                                }
                                _ => {
                                    return Err(CommandError::Protocol(
                                        "PING message must be a bulk string".to_string(),
                                    ))
                                }
                            }
                        } else {
                            None
                        };
                        Ok(Command::Ping { message })
                    }
                    "LENGTH" => {
                        if array.len() != 2 {
                            return Err(CommandError::WrongNumberOfArguments("LENGTH".to_string()));
                        }

                        let key = match &array[1] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "LENGTH key must be a bulk string".to_string(),
                                ))
                            }
                        };

                        Ok(Command::Length { key })
                    }
                    "KEYTIME" => {
                        if array.len() != 2 {
                            return Err(CommandError::WrongNumberOfArguments(
                                "KEYTIME".to_string(),
                            ));
                        }

                        let key = match &array[1] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "KEYTIME key must be a bulk string".to_string(),
                                ))
                            }
                        };

                        Ok(Command::KeyTime { key })
                    }
                    "AUTH" => {
                        if array.len() != 2 {
                            return Err(CommandError::WrongNumberOfArguments("AUTH".to_string()));
                        }

                        let password = match &array[1] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "AUTH password must be a bulk string".to_string(),
                                ))
                            }
                        };
                        Ok(Command::Auth { password })
                    }
                    "DBSIZE" => {
                        if array.len() != 1 {
                            return Err(CommandError::WrongNumberOfArguments("DBSIZE".to_string()));
                        }
                        Ok(Command::DBSize)
                    }
                    "SCAN" => {
                        if array.len() > 2 {
                            return Err(CommandError::WrongNumberOfArguments("SCAN".to_string()));
                        }

                        let cursor = if array.len() == 2 {
                            match &array[1] {
                                Frame::BulkString(bytes) => {
                                    let cursor_str = String::from_utf8_lossy(bytes).to_string();
                                    if cursor_str == "0" {
                                        None
                                    } else {
                                        Some(cursor_str)
                                    }
                                }
                                _ => {
                                    return Err(CommandError::Protocol(
                                        "SCAN cursor must be a bulk string".to_string(),
                                    ))
                                }
                            }
                        } else {
                            None
                        };

                        Ok(Command::Scan { cursor })
                    }
                    "RSCAN" => {
                        if array.len() > 2 {
                            return Err(CommandError::WrongNumberOfArguments("RSCAN".to_string()));
                        }

                        let cursor = if array.len() == 2 {
                            match &array[1] {
                                Frame::BulkString(bytes) => {
                                    let cursor_str = String::from_utf8_lossy(bytes).to_string();
                                    if cursor_str == "0" {
                                        None
                                    } else {
                                        Some(cursor_str)
                                    }
                                }
                                _ => {
                                    return Err(CommandError::Protocol(
                                        "RSCAN cursor must be a bulk string".to_string(),
                                    ))
                                }
                            }
                        } else {
                            None
                        };

                        Ok(Command::RScan { cursor })
                    }
                    "FLUSH" => {
                        if array.len() != 1 {
                            return Err(CommandError::WrongNumberOfArguments("FLUSH".to_string()));
                        }
                        Ok(Command::Flush)
                    }
                    "TIME" => {
                        if array.len() != 1 {
                            return Err(CommandError::WrongNumberOfArguments("TIME".to_string()));
                        }
                        Ok(Command::Time)
                    }
                    _ => Err(CommandError::UnknownCommand(command_name)),
                }
            }
            _ => Err(CommandError::Protocol(
                "Command must be an array".to_string(),
            )),
        }
    }
}

/// Handler for Redis commands
pub struct CommandHandler {
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
    pub fn new(
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
    pub fn set_namespace_authenticated(&mut self, authenticated: bool) {
        self.namespace_authenticated = authenticated;
        debug!("Set namespace authentication status to {}", authenticated);
    }
    /// Execute a command and return the response frame
    pub async fn execute(&self, cmd: Command) -> Frame {
        match cmd {
            Command::Get { key } => self.handle_get(key).await,
            Command::MGet { keys } => self.handle_mget(keys).await,
            Command::Set { key, value } => self.handle_set(key, value).await,
            Command::Ping { message } => self.handle_ping(message),
            Command::Del { key } => self.handle_del(key).await,
            Command::Exists { key } => self.handle_exists(key).await,
            Command::Check { key } => self.handle_check(key).await,
            Command::Length { key } => self.handle_length(key).await,
            Command::KeyTime { key } => self.handle_keytime(key).await,
            Command::NSNew { name } => self.handle_nsnew(name).await,
            Command::NSInfo { name } => self.handle_nsinfo(name).await,
            Command::NSList => self.handle_nslist().await,
            Command::DBSize => self.handle_dbsize(),
            Command::Scan { cursor } => self.handle_scan(cursor).await,
            Command::RScan { cursor } => self.handle_rscan(cursor).await,
            Command::NSSet {
                namespace,
                property,
                value,
            } => self.handle_nsset(namespace, property, value).await,
            Command::Flush => self.handle_flush().await,
            Command::Time => self.handle_time(),
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

    /// Handle GET command
    async fn handle_get(&self, key: String) -> Frame {
        debug!("Handling GET command for key: {}", key);

        // Check if namespace requires authentication for read operations
        let props = self.namespace.properties.read().unwrap();

        // If namespace is not public and user is not authenticated, deny access
        if !props.public && !self.namespace_authenticated {
            return Frame::Error("ERR Authentication required for read operations".into());
        }

        match self.namespace.get(key.as_bytes()) {
            Ok(Some(value)) => Frame::BulkString(value.to_vec()),
            Ok(None) => Frame::Null,
            Err(e) => {
                error!("Error getting key {}: {}", key, e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle MGET command - get multiple keys at once
    async fn handle_mget(&self, keys: Vec<String>) -> Frame {
        debug!("Handling MGET command for {} keys", keys.len());

        // Check if namespace requires authentication for read operations
        let props = self.namespace.properties.read().unwrap();

        // If namespace is not public and user is not authenticated, deny access
        if !props.public && !self.namespace_authenticated {
            return Frame::Error("ERR Authentication required for read operations".into());
        }

        let mut values = Vec::with_capacity(keys.len());

        for key in keys {
            match self.namespace.get(key.as_bytes()) {
                Ok(Some(value)) => values.push(Frame::BulkString(value.to_vec())),
                Ok(None) => values.push(Frame::Null),
                Err(e) => {
                    error!("Error getting key {}: {}", key, e);
                    // For MGET, we don't return an error for the whole command
                    // Instead, we return a null for this specific key
                    values.push(Frame::Null);
                }
            }
        }

        Frame::Array(values)
    }

    /// Handle SET command
    async fn handle_set(&self, key: String, value: Bytes) -> Frame {
        debug!("Handling SET command for key: {}", key);

        // Check if the connection is authenticated for this namespace
        if !self.namespace_authenticated {
            return Frame::Error("ERR: Authentication required for write operations".into());
        }

        match self.namespace.set(key.as_bytes(), value) {
            Ok(()) => Frame::SimpleString("OK".into()),
            Err(e) => {
                error!("Error setting key {}: {}", key, e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle PING command
    fn handle_ping(&self, message: Option<String>) -> Frame {
        match message {
            Some(msg) => Frame::BulkString(msg.into_bytes()),
            None => Frame::SimpleString("PONG".into()),
        }
    }

    /// Handle DEL command
    async fn handle_del(&self, key: String) -> Frame {
        debug!("Handling DEL command for key: {}", key);

        // Check if the connection is authenticated for this namespace
        if !self.namespace_authenticated {
            return Frame::Error("ERR: Authentication required for write operations".into());
        }

        match self.namespace.del(key.as_bytes()) {
            Ok(()) => Frame::Integer(1), // Successfully deleted 1 key
            Err(e) => {
                error!("Error deleting key {}: {}", key, e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle EXISTS command
    async fn handle_exists(&self, key: String) -> Frame {
        debug!("Handling EXISTS command for key: {}", key);

        // Check if namespace requires authentication for read operations
        let props = self.namespace.properties.read().unwrap();

        // If namespace is not public and user is not authenticated, deny access
        if !props.public && !self.namespace_authenticated {
            return Frame::Error("ERR Authentication required for read operations".into());
        }

        match self.namespace.exists(key.as_bytes()) {
            Ok(true) => Frame::Integer(1),  // Key exists
            Ok(false) => Frame::Integer(0), // Key does not exist
            Err(e) => {
                error!("Error checking if key {} exists: {}", key, e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle CHECK command - verify data integrity for a key
    async fn handle_check(&self, key: String) -> Frame {
        debug!("Handling CHECK command for key: {}", key);
        match self.namespace.check(key.as_bytes()) {
            Ok(Some(true)) => Frame::Integer(1), // Data integrity check passed
            Ok(Some(false)) | Ok(None) => Frame::Integer(0), // Check failed or key doesn't exist
            Err(e) => {
                error!("Error checking integrity for key {}: {}", key, e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle LENGTH command - get the size of a key's value
    async fn handle_length(&self, key: String) -> Frame {
        debug!("Handling LENGTH command for key: {}", key);
        match self.namespace.length(key.as_bytes()) {
            Ok(Some(size)) => Frame::Integer(size as i64), // Return the size as an integer
            Ok(None) => Frame::Null,                       // Key not found, return nil
            Err(e) => {
                error!("Error getting length for key {}: {}", key, e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle KEYTIME command - get the last-modified timestamp of a key
    async fn handle_keytime(&self, key: String) -> Frame {
        debug!("Handling KEYTIME command for key: {}", key);
        match self.namespace.keytime(key.as_bytes()) {
            Ok(Some(timestamp)) => Frame::Integer(timestamp), // Return the timestamp as an integer
            Ok(None) => Frame::Null,                          // Key not found, return nil
            Err(e) => {
                error!("Error getting timestamp for key {}: {}", key, e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle NSNEW command - create a new namespace
    /// This command requires admin privileges
    async fn handle_nsnew(&self, name: String) -> Frame {
        debug!("Handling NSNEW command for namespace: {}", name);

        // Check if the connection has admin privileges
        if !self.is_admin {
            error!("Unauthorized attempt to create namespace: {}", name);
            return Frame::Error("ERR NSNEW command requires admin privileges".into());
        }

        match self.storage.create_namespace(&name) {
            Ok(_) => Frame::SimpleString("OK".into()),
            Err(e) => {
                error!("Error creating namespace {}: {}", name, e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle NSINFO command - display information about a namespace
    async fn handle_nsinfo(&self, name: String) -> Frame {
        debug!("Handling NSINFO command for namespace: {}", name);
        match self.storage.get_namespace_meta(&name) {
            Ok(meta) => {
                // Format the namespace information as a multi-line string
                let info = format!(
                    "# namespace\nname: {}\npublic: {}\npassword: {}\ndata_limits_bytes: {}\nmode: {}\nworm: {}\nlocked: {}",
                    meta.name,
                    if meta.public { "yes" } else { "no" },
                    if meta.password.is_some() { "yes" } else { "no" },
                    meta.max_size.unwrap_or(0),
                    match meta.key_mode {
                        crate::storage::KeyMode::UserKey => "userkey",
                        crate::storage::KeyMode::Sequential => "sequential",
                    },
                    if meta.worm { "yes" } else { "no" },
                    if meta.locked { "yes" } else { "no" }
                );

                Frame::BulkString(info.into())
            }
            Err(e) => {
                error!("Error getting namespace info for {}: {}", name, e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle NSLIST command - list all namespaces
    async fn handle_nslist(&self) -> Frame {
        debug!("Handling NSLIST command");

        // NSLIST is available to all users, no admin check required

        // Get the list of namespaces
        match self.storage.iter_namespace() {
            Ok(namespaces) => {
                let mut result = Vec::new();
                for namespace in namespaces {
                    match namespace {
                        Ok(ns) => result.push(Frame::BulkString(ns.name.into_bytes())),
                        Err(_) => continue, // Skip namespaces with errors
                    }
                }

                // Return the array of namespace names
                Frame::Array(result)
            }
            Err(e) => {
                error!("Error listing namespaces: {}", e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle NSSET command - set a property for a namespace
    async fn handle_nsset(&self, namespace: String, property: String, value: String) -> Frame {
        debug!(
            "Handling NSSET command for namespace: {}, property: {}, value: {}",
            namespace, property, value
        );

        // Check if user has admin privileges
        if !self.is_admin {
            return Frame::Error("ERR NSSET command requires admin privileges".into());
        }

        // Get the namespace metadata
        match self.storage.get_namespace_meta(&namespace) {
            Ok(mut meta) => {
                // Update the property based on input
                match property.to_lowercase().as_str() {
                    "worm" => match value.parse::<BoolPropertyValue>() {
                        Ok(prop_value) => meta.worm = prop_value.to_bool(),
                        Err(e) => return Frame::Error(format!("ERR {}", e)),
                    },
                    "lock" => match value.parse::<BoolPropertyValue>() {
                        Ok(prop_value) => meta.locked = prop_value.to_bool(),
                        Err(e) => return Frame::Error(format!("ERR {}", e)),
                    },
                    "public" => match value.parse::<BoolPropertyValue>() {
                        Ok(prop_value) => meta.public = prop_value.to_bool(),
                        Err(e) => return Frame::Error(format!("ERR {}", e)),
                    },
                    "password" => {
                        // If value is empty string, remove password
                        if value.is_empty() {
                            meta.password = None;
                        } else {
                            meta.password = Some(value);
                        }
                    }
                    _ => return Frame::Error(format!("ERR Unknown property: {}", property)),
                }

                // Store the property values before moving meta
                let worm_value = meta.worm;
                let locked_value = meta.locked;
                let public_value = meta.public;

                // Persist the updated metadata
                match self.storage.update_namespace_meta(&namespace, meta) {
                    Ok(_) => {
                        // Use the shared namespace cache to update all instances of this namespace
                        // This ensures that all clients using this namespace will see the updated properties
                        self.namespace_cache.update_all_instances(&namespace, |ns| {
                            // Update the in-memory properties to reflect the change
                            let mut props = ns.properties.write().unwrap();
                            match property.to_lowercase().as_str() {
                                "worm" => props.worm = worm_value,
                                "lock" => props.locked = locked_value,
                                "public" => props.public = public_value,
                                _ => {} // Should never happen due to earlier check
                            }
                            debug!(
                                "Updated namespace {} in cache with property {}",
                                namespace, property
                            );
                        });

                        debug!(
                            "Using shared namespace cache to update all instances of namespace {}",
                            namespace
                        );

                        // Note: We don't need to separately update self.namespace
                        // because it should already be updated through the shared cache
                        // if it's the same namespace

                        Frame::SimpleString("OK".into())
                    }
                    Err(e) => {
                        Frame::Error(format!("ERR Failed to update namespace metadata: {}", e))
                    }
                }
            }
            Err(e) => Frame::Error(format!("ERR Namespace not found: {}", e)),
        }
    }

    /// Handle DBSIZE command - get the number of keys in the current namespace
    fn handle_dbsize(&self) -> Frame {
        debug!("Handling DBSIZE command");
        match self.namespace.num_keys() {
            Ok(count) => Frame::Integer(count as i64),
            Err(e) => Frame::Error(format!("ERR {e}")),
        }
    }

    /// Handle SCAN command - scan keys in the current namespace
    async fn handle_scan(&self, cursor: Option<String>) -> Frame {
        debug!("Handling SCAN command with cursor: {:?}", cursor);

        // Convert the cursor from String to Vec<u8> if it exists
        let start_after = cursor.map(|c| c.into_bytes());

        // Use the scan method to get keys starting after the cursor
        match self.namespace.scan(start_after, 10) {
            Ok(keys) => {
                if keys.is_empty() {
                    // If no keys were found, return 0 as cursor and empty array
                    let response = vec![Frame::BulkString("0".into()), Frame::Array(vec![])];
                    Frame::Array(response)
                } else {
                    // Determine the next cursor
                    // If we got fewer than 10 keys, we've reached the end
                    let next_cursor = if keys.len() < 10 {
                        "0".to_string()
                    } else {
                        // Otherwise, use the last key as the next cursor
                        let last_key = keys.last().unwrap();
                        String::from_utf8_lossy(last_key).to_string()
                    };

                    // Convert keys to frames
                    let key_frames: Vec<Frame> = keys.into_iter().map(Frame::BulkString).collect();

                    // Return [cursor, [keys...]]
                    let response = vec![
                        Frame::BulkString(next_cursor.into_bytes()),
                        Frame::Array(key_frames),
                    ];
                    Frame::Array(response)
                }
            }
            Err(e) => {
                error!("Error scanning keys: {}", e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle RSCAN command - scan keys in the current namespace in backward direction
    async fn handle_rscan(&self, cursor: Option<String>) -> Frame {
        debug!("Handling RSCAN command with cursor: {:?}", cursor);

        // Convert the cursor from String to Vec<u8> if it exists
        let start_after = cursor.map(|c| c.into_bytes());

        // Use the scan_backward method to get keys starting before the cursor
        match self.namespace.scan_backward(start_after, 10) {
            Ok(keys) => {
                if keys.is_empty() {
                    // If no keys were found, return 0 as cursor and empty array
                    let response = vec![Frame::BulkString("0".into()), Frame::Array(vec![])];
                    Frame::Array(response)
                } else {
                    // Determine the next cursor
                    // If we got fewer than 10 keys, we've reached the end
                    let next_cursor = if keys.len() < 10 {
                        "0".to_string()
                    } else {
                        // Otherwise, use the last key as the next cursor
                        let last_key = keys.last().unwrap();
                        String::from_utf8_lossy(last_key).to_string()
                    };

                    // Convert keys to frames
                    let key_frames: Vec<Frame> = keys.into_iter().map(Frame::BulkString).collect();

                    // Return [cursor, [keys...]]
                    let response = vec![
                        Frame::BulkString(next_cursor.into_bytes()),
                        Frame::Array(key_frames),
                    ];
                    Frame::Array(response)
                }
            }
            Err(e) => {
                error!("Error scanning keys: {}", e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle FLUSH command - delete all keys in the current namespace
    /// This command is only allowed on private and password protected namespaces
    async fn handle_flush(&self) -> Frame {
        debug!("Handling FLUSH command");

        // Get namespace properties
        let props = self.namespace.properties.read().unwrap();
        let namespace_name = props.namespace_name.clone();

        // Check if the namespace is private (not public)
        if props.public {
            return Frame::Error("ERR: FLUSH command is only allowed on private namespaces".into());
        }

        // Check if the namespace is password-protected by checking if it has a password
        let has_password = match self.storage.get_namespace_meta(&namespace_name) {
            Ok(meta) => meta.password.is_some(),
            Err(_) => false,
        };

        if !has_password {
            return Frame::Error(
                "ERR: FLUSH command is only allowed on password-protected namespaces".into(),
            );
        }

        // Check if the connection is authenticated for this namespace
        if !self.namespace_authenticated {
            return Frame::Error("ERR: Authentication required for FLUSH command".into());
        }

        // Execute the flush operation
        match self.namespace.flush(self.namespace_cache.as_ref()) {
            Ok(_) => Frame::SimpleString("OK".into()),
            Err(e) => {
                error!("Error flushing namespace: {}", e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle TIME command - returns the current server time as a two-element array: [seconds, microseconds]
    /// This is fully compatible with Redis TIME command
    fn handle_time(&self) -> Frame {
        use std::time::{SystemTime, UNIX_EPOCH};

        debug!("Handling TIME command");

        // Get the current system time
        let now = SystemTime::now();

        // Convert to duration since UNIX epoch
        match now.duration_since(UNIX_EPOCH) {
            Ok(duration) => {
                // Extract seconds and microseconds
                let seconds = duration.as_secs();
                let microseconds = duration.subsec_micros();

                // Create response array with two elements: [seconds, microseconds]
                let response = vec![
                    Frame::BulkString(seconds.to_string().into_bytes()),
                    Frame::BulkString(microseconds.to_string().into_bytes()),
                ];

                Frame::Array(response)
            }
            Err(_) => {
                // This should never happen unless the system clock is set before UNIX epoch
                Frame::Error("ERR Failed to get system time".into())
            }
        }
    }
}
