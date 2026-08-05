use bytes::Bytes;
use redis_protocol::resp2::types::OwnedFrame as Frame;
use tracing::{debug, error};

use crate::content::{self, Ingest};
use crate::storage::KeyMode;

use super::{CommandHandler, shown};

impl CommandHandler {
    /// Handle GET command
    pub(super) async fn handle_get(&self, key: &[u8]) -> Frame {
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
    pub(super) async fn handle_mget(&self, keys: Vec<Bytes>) -> Frame {
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
    pub(super) async fn handle_set(&self, key: Bytes, value: Bytes) -> Frame {
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
    pub(super) async fn handle_cset(&self, value: Bytes) -> Frame {
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
    pub(super) fn handle_ping(message: Option<String>) -> Frame {
        match message {
            Some(msg) => Frame::BulkString(msg.into_bytes()),
            None => Frame::SimpleString("PONG".into()),
        }
    }

    /// Handle ECHO command
    ///
    /// Needs no namespace and touches no storage, so it answers before any
    /// authentication check -- same as PING.
    pub(super) fn handle_echo(message: Bytes) -> Frame {
        debug!("Handling ECHO command for {} bytes", message.len());
        Frame::BulkString(message.to_vec())
    }

    /// Handle DEL command
    pub(super) async fn handle_del(&self, keys: Vec<Bytes>) -> Frame {
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
    pub(super) fn handle_exists(&self, key: &[u8]) -> Frame {
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
    pub(super) async fn handle_check(&self, key: &[u8]) -> Frame {
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
    pub(super) fn handle_length(&self, key: &[u8]) -> Frame {
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
    pub(super) fn handle_keytime(&self, key: &[u8]) -> Frame {
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

    /// Handle TIME command - returns the current server time as a two-element array: [seconds, microseconds]
    /// This is fully compatible with Redis TIME command
    pub(super) fn handle_time() -> Frame {
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
