use bytes::Bytes;
use redis_protocol::resp2::types::OwnedFrame as Frame;
use tracing::{debug, error};

use crate::property::BoolPropertyValue;

use super::parse::{key_mode_name, parse_key_mode, parse_max_size};
use super::{CommandHandler, SCAN_PAGE};

impl CommandHandler {
    /// Handle NSNEW command - create a new namespace
    /// This command requires admin privileges
    pub(super) fn handle_nsnew(&self, name: String) -> Frame {
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
    pub(super) fn handle_nsinfo(&self, name: String) -> Frame {
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
    pub(super) fn handle_nslist(&self) -> Frame {
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
    pub(super) fn handle_nsset(&self, namespace: String, property: String, value: String) -> Frame {
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
                    "max_size" => match parse_max_size(&value) {
                        Ok(limit) => meta.max_size = limit,
                        Err(e) => return Frame::Error(format!("ERR {e}")),
                    },
                    _ => return Frame::Error(format!("ERR Unknown property: {}", property)),
                }

                // Store the property values before moving meta
                let worm_value = meta.worm;
                let locked_value = meta.locked;
                let public_value = meta.public;
                let max_size_value = meta.max_size;

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
                                // Every connection already on this namespace
                                // must see the new limit: it is what their
                                // next write is measured against.
                                "max_size" => props.max_size = max_size_value,
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
    pub(super) fn handle_dbsize(&self) -> Frame {
        debug!("Handling DBSIZE command");
        match self.namespace.num_keys() {
            Ok(count) => Frame::Integer(count as i64),
            Err(e) => Frame::Error(format!("ERR {e}")),
        }
    }

    /// Handle SCAN command - scan keys in the current namespace
    pub(super) fn handle_scan(&self, cursor: Option<Bytes>) -> Frame {
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
    pub(super) fn handle_rscan(&self, cursor: Option<Bytes>) -> Frame {
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
    pub(super) fn handle_flush(&self) -> Frame {
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
}
