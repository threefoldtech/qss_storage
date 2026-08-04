use bytes::Bytes;
use redis_protocol::resp2::types::OwnedFrame as Frame;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error};

use crate::content::{self, Ingest};
use crate::namespace::{Namespace, NamespaceCache};
use crate::property::BoolPropertyValue;
use crate::storage::{KeyMode, Storage};

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

/// A command frame split into its name and argument frames.
///
/// `argv[0]` is the command name itself, so argument indices below match the
/// wire positions (`argv[1]` is the first real argument) and all arity numbers
/// count the command name.
struct Args {
    /// Upper-cased command name, also used verbatim in error messages.
    name: String,
    argv: Vec<Frame>,
}

impl Args {
    /// Split a frame into name plus arguments, rejecting non-command frames.
    fn from_frame(frame: Frame) -> Result<Self, CommandError> {
        let argv = match frame {
            Frame::Array(array) => array,
            _ => {
                return Err(CommandError::Protocol(
                    "Command must be an array".to_string(),
                ));
            }
        };

        if argv.is_empty() {
            return Err(CommandError::WrongNumberOfArguments(
                "empty command".to_string(),
            ));
        }

        let name = match &argv[0] {
            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_uppercase(),
            _ => {
                return Err(CommandError::Protocol(
                    "Command name must be a bulk string".to_string(),
                ));
            }
        };

        Ok(Self { name, argv })
    }

    fn len(&self) -> usize {
        self.argv.len()
    }

    fn wrong_arity(&self) -> CommandError {
        CommandError::WrongNumberOfArguments(self.name.clone())
    }

    /// Require exactly `n` frames (command name included).
    fn arity_exact(&self, n: usize) -> Result<(), CommandError> {
        if self.len() == n {
            Ok(())
        } else {
            Err(self.wrong_arity())
        }
    }

    /// Require at least `n` frames (command name included).
    fn arity_min(&self, n: usize) -> Result<(), CommandError> {
        if self.len() >= n {
            Ok(())
        } else {
            Err(self.wrong_arity())
        }
    }

    /// Require at most `n` frames (command name included).
    fn arity_max(&self, n: usize) -> Result<(), CommandError> {
        if self.len() <= n {
            Ok(())
        } else {
            Err(self.wrong_arity())
        }
    }

    /// Require between `min` and `max` frames (command name included).
    fn arity_range(&self, min: usize, max: usize) -> Result<(), CommandError> {
        self.arity_min(min)?;
        self.arity_max(max)
    }

    /// Read the bulk string at `idx` as raw bytes. `what` names the argument in
    /// the error message, e.g. `SET value must be a bulk string`.
    fn bytes_at(&self, idx: usize, what: &str) -> Result<Bytes, CommandError> {
        match &self.argv[idx] {
            Frame::BulkString(bytes) => Ok(Bytes::from(bytes.clone())),
            _ => Err(CommandError::Protocol(format!(
                "{} {} must be a bulk string",
                self.name, what
            ))),
        }
    }

    /// Read the bulk string at `idx` as a lossy UTF-8 `String`.
    fn string_at(&self, idx: usize, what: &str) -> Result<String, CommandError> {
        match &self.argv[idx] {
            Frame::BulkString(bytes) => Ok(String::from_utf8_lossy(bytes).to_string()),
            _ => Err(CommandError::Protocol(format!(
                "{} {} must be a bulk string",
                self.name, what
            ))),
        }
    }

    /// Same as `string_at`, but yields `None` when the argument was not sent.
    fn opt_string_at(&self, idx: usize, what: &str) -> Result<Option<String>, CommandError> {
        if idx < self.len() {
            Ok(Some(self.string_at(idx, what)?))
        } else {
            Ok(None)
        }
    }

    /// Same as `bytes_at`, but yields `None` when the argument was not sent.
    fn opt_bytes_at(&self, idx: usize, what: &str) -> Result<Option<Bytes>, CommandError> {
        if idx < self.len() {
            Ok(Some(self.bytes_at(idx, what)?))
        } else {
            Ok(None)
        }
    }
}

/// `<CMD>` with no arguments: DBSIZE, FLUSH, NSLIST, TIME.
fn parse_no_args(args: &Args, cmd: Command) -> Result<Command, CommandError> {
    args.arity_exact(1)?;
    Ok(cmd)
}

/// `<CMD> <arg>`: AUTH, NSINFO, NSNEW -- the commands whose argument is a
/// name rather than a key.
fn parse_one_arg<F>(args: &Args, what: &str, make: F) -> Result<Command, CommandError>
where
    F: FnOnce(String) -> Command,
{
    args.arity_exact(2)?;
    Ok(make(args.string_at(1, what)?))
}

/// `<CMD> <key>`: CHECK, EXISTS, GET, KEYTIME, LENGTH. The key stays bytes;
/// see [`Command`].
fn parse_one_key<F>(args: &Args, make: F) -> Result<Command, CommandError>
where
    F: FnOnce(Bytes) -> Command,
{
    args.arity_exact(2)?;
    Ok(make(args.bytes_at(1, "key")?))
}

/// `<CMD> [cursor]`: SCAN, RSCAN. Cursor "0" means "start at the beginning of
/// the walk" -- the smallest key for SCAN and the LARGEST for RSCAN, since a
/// reverse walk begins at the end of the tree (zdb's semantics, and the
/// mirror of SCAN's). Any other value is a key to resume past, so it is bytes
/// for the same reason keys are.
fn parse_cursor<F>(args: &Args, make: F) -> Result<Command, CommandError>
where
    F: FnOnce(Option<Bytes>) -> Command,
{
    args.arity_max(2)?;
    let cursor = args
        .opt_bytes_at(1, "cursor")?
        .filter(|cursor| cursor.as_ref() != b"0");
    Ok(make(cursor))
}

/// `MGET key [key ...]`
fn parse_mget(args: &Args) -> Result<Command, CommandError> {
    Ok(Command::MGet {
        keys: parse_keys(args)?,
    })
}

/// `DEL key [key ...]` -- variadic like Redis, replying the summed count.
fn parse_del(args: &Args) -> Result<Command, CommandError> {
    Ok(Command::Del {
        keys: parse_keys(args)?,
    })
}

/// Every argument after the command name as a key, at least one.
fn parse_keys(args: &Args) -> Result<Vec<Bytes>, CommandError> {
    args.arity_min(2)?;
    let mut keys = Vec::with_capacity(args.len() - 1);
    for idx in 1..args.len() {
        keys.push(args.bytes_at(idx, "key")?);
    }
    Ok(keys)
}

/// `SET key value` (trailing arguments are accepted and ignored)
///
/// An empty key is a value in a content-addressed namespace -- the zdb-shaped
/// "you compute the key" form (ADR 0014) -- and an ordinary key everywhere
/// else, so the parser passes it through and the handler decides.
fn parse_set(args: &Args) -> Result<Command, CommandError> {
    args.arity_min(3)?;
    let key = args.bytes_at(1, "key")?;
    let value = args.bytes_at(2, "value")?;
    Ok(Command::Set { key, value })
}

/// `CSET value`: SET with the key left to the server, spelled as its own verb.
fn parse_cset(args: &Args) -> Result<Command, CommandError> {
    args.arity_exact(2)?;
    Ok(Command::CSet {
        value: args.bytes_at(1, "value")?,
    })
}

/// `PING [message]`
fn parse_ping(args: &Args) -> Result<Command, CommandError> {
    let message = args.opt_string_at(1, "message")?;
    Ok(Command::Ping { message })
}

/// `ECHO message`
///
/// The payload stays `Bytes` rather than becoming a lossy `String`: a client
/// may echo bytes that are not UTF-8, and `valkey-cli --pipe` in fact does --
/// it ends its stream with an ECHO of twenty random bytes and waits for them
/// back verbatim. Decoding lossily here would answer with replacement
/// characters and leave that client waiting out its timeout.
fn parse_echo(args: &Args) -> Result<Command, CommandError> {
    args.arity_exact(2)?;
    let message = args.bytes_at(1, "message")?;
    Ok(Command::Echo { message })
}

/// `SELECT namespace [password]`
fn parse_select(args: &Args) -> Result<Command, CommandError> {
    args.arity_range(2, 3)?;
    let namespace = args.string_at(1, "namespace")?;
    let password = args.opt_string_at(2, "password")?;
    Ok(Command::Select {
        namespace,
        password,
    })
}

/// How a key mode is spelled on the wire: what `NSINFO` prints and what
/// `NSSET <ns> key_mode <value>` accepts.
fn key_mode_name(mode: KeyMode) -> &'static str {
    match mode {
        KeyMode::UserKey => "userkey",
        KeyMode::Sequential => "sequential",
        KeyMode::Cas => "cas",
    }
}

/// The key mode `value` names, or an error naming the ones that exist.
///
/// `sequential` is refused rather than accepted: it is a zdb-heritage variant
/// nothing here implements, so setting it would leave a namespace whose
/// behaviour is undefined. It stays in [`KeyMode`] because it is on disk in
/// stores that were created with it.
fn parse_key_mode(value: &str) -> Result<KeyMode, String> {
    match value.to_lowercase().as_str() {
        "userkey" => Ok(KeyMode::UserKey),
        "cas" => Ok(KeyMode::Cas),
        "sequential" => Err(
            "the sequential key mode is not implemented; namespaces are userkey or cas".to_string(),
        ),
        other => Err(format!(
            "unknown key mode: {other} (expected userkey or cas)"
        )),
    }
}

/// `NSSET namespace property value`
fn parse_nsset(args: &Args) -> Result<Command, CommandError> {
    args.arity_exact(4)?;
    let namespace = args.string_at(1, "namespace")?;
    let property = args.string_at(2, "property")?;
    let value = args.string_at(3, "value")?;
    Ok(Command::NSSet {
        namespace,
        property,
        value,
    })
}

impl Command {
    /// Parse a Redis protocol frame into a command
    pub fn from_frame(frame: Frame) -> Result<Self, CommandError> {
        let args = Args::from_frame(frame)?;

        match args.name.as_str() {
            "AUTH" => parse_one_arg(&args, "password", |password| Command::Auth { password }),
            "CHECK" => parse_one_key(&args, |key| Command::Check { key }),
            "CSET" => parse_cset(&args),
            "DBSIZE" => parse_no_args(&args, Command::DBSize),
            "DEL" => parse_del(&args),
            "ECHO" => parse_echo(&args),
            "EXISTS" => parse_one_key(&args, |key| Command::Exists { key }),
            "FLUSH" => parse_no_args(&args, Command::Flush),
            "GET" => parse_one_key(&args, |key| Command::Get { key }),
            "KEYTIME" => parse_one_key(&args, |key| Command::KeyTime { key }),
            "LENGTH" => parse_one_key(&args, |key| Command::Length { key }),
            "MGET" => parse_mget(&args),
            "NSINFO" => parse_one_arg(&args, "name", |name| Command::NSInfo { name }),
            "NSLIST" => parse_no_args(&args, Command::NSList),
            "NSNEW" => parse_one_arg(&args, "name", |name| Command::NSNew { name }),
            "NSSET" => parse_nsset(&args),
            "PING" => parse_ping(&args),
            "RSCAN" => parse_cursor(&args, |cursor| Command::RScan { cursor }),
            "SCAN" => parse_cursor(&args, |cursor| Command::Scan { cursor }),
            "SELECT" => parse_select(&args),
            "SET" => parse_set(&args),
            "TIME" => parse_no_args(&args, Command::Time),
            _ => Err(CommandError::UnknownCommand(args.name)),
        }
    }
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

    /// Handle GET command
    async fn handle_get(&self, key: &[u8]) -> Frame {
        debug!("Handling GET command for key: {}", shown(key));

        // If namespace is not public and user is not authenticated, deny access
        if !self.may_read() {
            return Frame::Error("ERR Authentication required for read operations".into());
        }

        match self.namespace.get(key).await {
            Ok(Some(value)) => Frame::BulkString(value.to_vec()),
            Ok(None) => Frame::Null,
            Err(e) => {
                error!("Error getting key {}: {}", shown(key), e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle MGET command - get multiple keys at once
    async fn handle_mget(&self, keys: Vec<Bytes>) -> Frame {
        debug!("Handling MGET command for {} keys", keys.len());

        // If namespace is not public and user is not authenticated, deny access
        if !self.may_read() {
            return Frame::Error("ERR Authentication required for read operations".into());
        }

        let mut values = Vec::with_capacity(keys.len());

        for key in keys {
            match self.namespace.get(&key).await {
                Ok(Some(value)) => values.push(Frame::BulkString(value.to_vec())),
                Ok(None) => values.push(Frame::Null),
                Err(e) => {
                    error!("Error getting key {}: {}", shown(&key), e);
                    // For MGET, we don't return an error for the whole command
                    // Instead, we return a null for this specific key
                    values.push(Frame::Null);
                }
            }
        }

        Frame::Array(values)
    }

    /// Handle SET command
    ///
    /// What a key means decides what this does (ADR 0014). In a user-keyed
    /// namespace it is the store-what-I-say write it has always been. In a
    /// content-addressed one the key is the value's address: empty means
    /// "compute it and tell me", 32 bytes means "I claim this address", and
    /// anything else is an error.
    async fn handle_set(&self, key: Bytes, value: Bytes) -> Frame {
        debug!("Handling SET command for key: {}", shown(&key));

        // Check if the connection is authenticated for this namespace
        if !self.namespace_authenticated {
            return Frame::Error("ERR: Authentication required for write operations".into());
        }

        if self.namespace.key_mode() == KeyMode::Cas {
            let mode = if key.is_empty() {
                Ingest::ServerHashed
            } else {
                Ingest::ClientHashed(&key)
            };
            return self.ingest(mode, value).await;
        }

        match self.namespace.set(&key, value).await {
            Ok(()) => Frame::SimpleString("OK".into()),
            Err(e) => {
                error!("Error setting key {}: {}", shown(&key), e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle CSET command - the server-hashed put (ADR 0014)
    ///
    /// The same operation as `SET "" <value>`, spelled as a verb of its own
    /// for clients that would rather say what they mean than send an empty
    /// key. It exists only where a key is an address.
    async fn handle_cset(&self, value: Bytes) -> Frame {
        debug!("Handling CSET command for {} bytes", value.len());

        if !self.namespace_authenticated {
            return Frame::Error("ERR: Authentication required for write operations".into());
        }

        if self.namespace.key_mode() != KeyMode::Cas {
            return Frame::Error(
                "ERR CSET is only valid in a content-addressed namespace \
                 (NSSET <namespace> key_mode cas)"
                    .into(),
            );
        }

        self.ingest(Ingest::ServerHashed, value).await
    }

    /// The write half both content-addressed forms share.
    ///
    /// The reply is the difference between them: a client that had the
    /// server compute the key gets it back as a bulk string, and one that
    /// named the address it was writing to already has it, so it gets `+OK`.
    async fn ingest(&self, mode: Ingest<'_>, value: Bytes) -> Frame {
        let derived = matches!(mode, Ingest::ServerHashed);

        match content::ingest(&self.storage, &self.namespace, mode, value).await {
            Ok(key) if derived => Frame::BulkString(key.to_vec()),
            Ok(_) => Frame::SimpleString("OK".into()),
            Err(e) => {
                error!("Error storing a content-addressed value: {}", e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle PING command
    fn handle_ping(message: Option<String>) -> Frame {
        match message {
            Some(msg) => Frame::BulkString(msg.into_bytes()),
            None => Frame::SimpleString("PONG".into()),
        }
    }

    /// Handle ECHO command
    ///
    /// Needs no namespace and touches no storage, so it answers before any
    /// authentication check -- same as PING.
    fn handle_echo(message: Bytes) -> Frame {
        debug!("Handling ECHO command for {} bytes", message.len());
        Frame::BulkString(message.to_vec())
    }

    /// Handle DEL command
    async fn handle_del(&self, keys: Vec<Bytes>) -> Frame {
        debug!("Handling DEL command for {} keys", keys.len());

        // Check if the connection is authenticated for this namespace
        if !self.namespace_authenticated {
            return Frame::Error("ERR: Authentication required for write operations".into());
        }

        // Redis semantics: the reply counts the keys that existed and were
        // removed, not the keys that were named.
        let mut removed: i64 = 0;
        for key in &keys {
            match self.namespace.del(key).await {
                Ok(true) => removed += 1,
                Ok(false) => {}
                Err(e) => {
                    error!("Error deleting key {}: {}", shown(key), e);
                    return Frame::Error(format!("ERR {}", e));
                }
            }
        }
        Frame::Integer(removed)
    }

    /// Handle EXISTS command
    ///
    /// The probe a dedup-upload client pipelines before it transfers
    /// anything (ADR 0014). Namespace-scoped, and advisory outside a worm
    /// namespace: nothing stops another client deleting the key between the
    /// answer and the upload the client then skips.
    fn handle_exists(&self, key: &[u8]) -> Frame {
        debug!("Handling EXISTS command for key: {}", shown(key));

        // If namespace is not public and user is not authenticated, deny access
        if !self.may_read() {
            return Frame::Error("ERR Authentication required for read operations".into());
        }

        match self.namespace.exists(key) {
            Ok(true) => Frame::Integer(1),  // Key exists
            Ok(false) => Frame::Integer(0), // Key does not exist
            Err(e) => {
                error!("Error checking if key {} exists: {}", shown(key), e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle CHECK command - verify data integrity for a key
    async fn handle_check(&self, key: &[u8]) -> Frame {
        debug!("Handling CHECK command for key: {}", shown(key));
        match self.namespace.check(key).await {
            Ok(Some(true)) => Frame::Integer(1), // Data integrity check passed
            Ok(Some(false)) | Ok(None) => Frame::Integer(0), // Check failed or key doesn't exist
            Err(e) => {
                error!("Error checking integrity for key {}: {}", shown(key), e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle LENGTH command - get the size of a key's value
    fn handle_length(&self, key: &[u8]) -> Frame {
        debug!("Handling LENGTH command for key: {}", shown(key));
        match self.namespace.length(key) {
            Ok(Some(size)) => Frame::Integer(size as i64), // Return the size as an integer
            Ok(None) => Frame::Null,                       // Key not found, return nil
            Err(e) => {
                error!("Error getting length for key {}: {}", shown(key), e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle KEYTIME command - get the last-modified timestamp of a key
    fn handle_keytime(&self, key: &[u8]) -> Frame {
        debug!("Handling KEYTIME command for key: {}", shown(key));
        match self.namespace.keytime(key) {
            Ok(Some(timestamp)) => Frame::Integer(timestamp), // Return the timestamp as an integer
            Ok(None) => Frame::Null,                          // Key not found, return nil
            Err(e) => {
                error!("Error getting timestamp for key {}: {}", shown(key), e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle NSNEW command - create a new namespace
    /// This command requires admin privileges
    fn handle_nsnew(&self, name: String) -> Frame {
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
    ///
    /// `data_size_bytes` is what the namespace holds and `data_limits_bytes`
    /// what it may hold (`0` for no limit) -- the pair an operator needs to
    /// answer "how full is it", spelled the way zdb spells them.
    fn handle_nsinfo(&self, name: String) -> Frame {
        debug!("Handling NSINFO command for namespace: {}", name);
        match self.storage.get_namespace_meta(&name) {
            Ok(meta) => {
                let usage = match self.storage.namespace_usage(&name) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        error!("Error reading the usage of namespace {}: {}", name, e);
                        return Frame::Error(format!("ERR {}", e));
                    }
                };

                // Format the namespace information as a multi-line string
                let info = format!(
                    "# namespace\nname: {}\npublic: {}\npassword: {}\ndata_size_bytes: {}\ndata_limits_bytes: {}\nmode: {}\nworm: {}\nlocked: {}",
                    meta.name,
                    if meta.public { "yes" } else { "no" },
                    if meta.password.is_some() { "yes" } else { "no" },
                    usage,
                    meta.max_size.unwrap_or(0),
                    key_mode_name(meta.key_mode),
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
    fn handle_nslist(&self) -> Frame {
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
    fn handle_nsset(&self, namespace: String, property: String, value: String) -> Frame {
        debug!(
            "Handling NSSET command for namespace: {}, property: {}, value: {}",
            namespace, property, value
        );

        // Check if user has admin privileges
        if !self.is_admin {
            return Frame::Error("ERR NSSET command requires admin privileges".into());
        }

        // The key mode is not a field to overwrite like the others: it is
        // only coherent on an empty namespace, and switching to `cas` raises
        // the store's header first (ADR 0014). Both live in the storage
        // layer, so this property takes its own path.
        if property.eq_ignore_ascii_case("key_mode") {
            return self.handle_nsset_key_mode(&namespace, &value);
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

    /// `NSSET <ns> key_mode <userkey|cas>` (ADR 0014).
    ///
    /// Refused on a namespace that holds keys, with the count in the message:
    /// the existing keys would stop meaning what they say, and there is no
    /// migration that could make them mean the other thing.
    fn handle_nsset_key_mode(&self, namespace: &str, value: &str) -> Frame {
        let key_mode = match parse_key_mode(value) {
            Ok(key_mode) => key_mode,
            Err(e) => return Frame::Error(format!("ERR {e}")),
        };

        match self.storage.set_key_mode(namespace, key_mode) {
            Ok(()) => {
                // Every connection already bound to this namespace must see
                // the new mode: it decides what SET means.
                self.namespace_cache.update_all_instances(namespace, |ns| {
                    ns.properties.write().unwrap().key_mode = key_mode;
                });
                Frame::SimpleString("OK".into())
            }
            Err(e) => Frame::Error(format!("ERR {e}")),
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
    fn handle_scan(&self, cursor: Option<Bytes>) -> Frame {
        debug!("Handling SCAN command");

        // Use the scan method to get keys starting after the cursor
        match self
            .namespace
            .scan(cursor.map(|c| c.to_vec()), SCAN_PAGE as u32)
        {
            Ok(keys) => Self::scan_reply(keys),
            Err(e) => {
                error!("Error scanning keys: {}", e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle RSCAN command - scan keys in the current namespace in backward
    /// direction, largest key first
    ///
    /// `RSCAN 0` starts the walk at the largest key the namespace holds, and
    /// each page's cursor resumes strictly below itself -- the mirror of
    /// SCAN, and what a zdb-shaped client expects.
    fn handle_rscan(&self, cursor: Option<Bytes>) -> Frame {
        debug!("Handling RSCAN command");

        // Use the scan_backward method to get keys starting before the cursor
        match self
            .namespace
            .scan_backward(cursor.map(|c| c.to_vec()), SCAN_PAGE as u32)
        {
            Ok(keys) => Self::scan_reply(keys),
            Err(e) => {
                error!("Error scanning keys: {}", e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// `[cursor, [key ...]]`, with `0` for a page that reached the end.
    ///
    /// The cursor is the last key of a full page, handed back as the bytes
    /// it is: in a content-addressed namespace a key is a hash, and a cursor
    /// that had been through a lossy UTF-8 decode would resume the scan
    /// somewhere else entirely (ADR 0014).
    fn scan_reply(keys: Vec<Vec<u8>>) -> Frame {
        let next_cursor = if keys.len() < SCAN_PAGE {
            b"0".to_vec()
        } else {
            keys.last().expect("a full page has a last key").clone()
        };

        let key_frames: Vec<Frame> = keys.into_iter().map(Frame::BulkString).collect();
        Frame::Array(vec![
            Frame::BulkString(next_cursor),
            Frame::Array(key_frames),
        ])
    }

    /// Handle FLUSH command - delete all keys in the current namespace
    /// This command is only allowed on private and password protected namespaces
    fn handle_flush(&self) -> Frame {
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
    fn handle_time() -> Frame {
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
