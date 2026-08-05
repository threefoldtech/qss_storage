use bytes::Bytes;
use redis_protocol::resp2::types::OwnedFrame as Frame;

use crate::storage::KeyMode;

use super::{Command, CommandError};

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
pub(super) fn key_mode_name(mode: KeyMode) -> &'static str {
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
pub(super) fn parse_key_mode(value: &str) -> Result<KeyMode, String> {
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

/// The limit `NSSET <ns> max_size <bytes>` names, in logical bytes.
///
/// `0` is not a limit of zero, it is the absence of one: that is the
/// zdb-heritage spelling NSINFO already prints for an unbounded namespace
/// (`data_limits_bytes: 0`), and a namespace that could hold nothing at all
/// would be a namespace nobody could ask for.
pub(super) fn parse_max_size(value: &str) -> Result<Option<u64>, String> {
    match value.trim().parse::<u64>() {
        Ok(0) => Ok(None),
        Ok(bytes) => Ok(Some(bytes)),
        Err(_) => Err(format!(
            "Invalid property value: {value} (max_size is a number of bytes, 0 for no limit)"
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
