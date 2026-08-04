//! What SET means when the key is the value's hash (ADR 0014).
//!
//! A content-addressed namespace keys every record by the BLAKE3-256 of its
//! value: 32 raw bytes on the wire, which RESP carries as it carries any
//! other bulk string. There are two ways in and one way through:
//!
//! - `SET "" <value>` and `CSET <value>` -- the server hashes and replies
//!   with the key it computed;
//! - `SET <32-byte key> <value>` -- the client already knows the address,
//!   usually because it asked `EXISTS` first and got a miss.
//!
//! # Presence is checked before anything is verified or written
//!
//! ```text
//! presence(H):
//!   this namespace has H        -> ack; the bytes are discarded, nothing changes
//!   another cas namespace has H -> clone the record by reference (same blocks,
//!                                  one more reference each; inline: the stored,
//!                                  already-verified bytes), discard the bytes
//!   nowhere                     -> the keyed form verifies blake3(value) == H,
//!                                  then stores
//! ```
//!
//! The invariant that buys is: bytes are verified against their address
//! exactly once, on the write that first materializes them. Everything
//! afterwards is served from content that already passed. So `GET H` can
//! only ever return bytes that hash to `H`, and a client that sends the
//! wrong bytes under an address that is already present is answered `+OK`
//! rather than an error -- the deliberate casualty of not hashing data that
//! is about to be discarded (ADR 0014, ruling 4).
//!
//! # Only a content-addressed namespace is a source
//!
//! The cross-namespace lookup enumerates namespaces whose key mode is
//! [`KeyMode::Cas`](crate::storage::KeyMode::Cas), and no others. A 32-byte
//! key in a user-keyed namespace is a coincidence: nothing ever checked that
//! its value hashes to it, so cloning from it would file unverified bytes
//! under an address.

use anyhow::{Result, anyhow};
use bytes::Bytes;
use tracing::debug;

use crate::namespace::Namespace;
use crate::storage::Storage;

/// Length of a content address on the wire: BLAKE3-256, raw.
///
/// Not the store's block address width (which may be 16, and is a different
/// address space entirely -- see `cas_storage::metastore::block`). This is
/// the whole-value digest a client can compute with stock `b3sum`, which is
/// the reason it is fixed at 32 rather than following the store's header.
pub(crate) const CAS_KEY_LEN: usize = 32;

/// The address of a value: BLAKE3-256 over the whole thing.
pub fn value_key(value: &[u8]) -> [u8; CAS_KEY_LEN] {
    *blake3::hash(value).as_bytes()
}

/// How the client named the record it is writing.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Ingest<'a> {
    /// `SET "" <value>` or `CSET <value>`: the server derives the key.
    /// Hashing IS the verification -- a derived key cannot mismatch.
    ServerHashed,
    /// `SET <32-byte key> <value>`: the client claims an address, which is
    /// verified on the write that first materializes it.
    ClientHashed(&'a [u8]),
}

/// Stores `value` in a content-addressed namespace and answers with its
/// address.
///
/// The returned key is what a mode-A reply carries; a mode-B caller already
/// has it and replies `+OK`.
///
/// # Errors
///
/// A claimed key that is not [`CAS_KEY_LEN`] bytes, a claimed key that the
/// value does not hash to (only ever checked when the address is nowhere in
/// the store), or a storage failure.
pub(crate) async fn ingest(
    storage: &Storage,
    namespace: &Namespace,
    mode: Ingest<'_>,
    value: Bytes,
) -> Result<[u8; CAS_KEY_LEN]> {
    let key = match mode {
        Ingest::ServerHashed => value_key(&value),
        Ingest::ClientHashed(claimed) => {
            let claimed: [u8; CAS_KEY_LEN] = claimed.try_into().map_err(|_| {
                anyhow!(
                    "a key in a cas namespace is {CAS_KEY_LEN} bytes (the BLAKE3 of the \
                     value), or empty to have the server compute it"
                )
            })?;
            claimed
        }
    };

    // Presence in this namespace: the record is already here and its content
    // was verified when it was written. Pure acknowledgement -- no write, no
    // reference count change, and the incoming bytes are dropped unread.
    if namespace.exists(&key)? {
        debug!(
            key = %hex(&key),
            "cas SET: the address is already in this namespace; nothing to do"
        );
        return Ok(key);
    }

    // Presence in another content-addressed namespace: the content is
    // implied to be in the store already, so this namespace takes a
    // reference to it rather than a copy.
    if clone_from_another_namespace(storage, namespace, &key).await? {
        return Ok(key);
    }

    // A miss everywhere: this write is the one that materializes the bytes,
    // so it is the one that must verify them.
    if let Ingest::ClientHashed(_) = mode {
        let actual = value_key(&value);
        if actual != key {
            return Err(anyhow!(
                "the value does not hash to the key it was sent under (blake3 is {}, \
                 the key says {})",
                hex(&actual),
                hex(&key)
            ));
        }
    }

    namespace.store_verified(&key, value).await?;
    Ok(key)
}

/// Looks for `key` in every OTHER content-addressed namespace and, on a hit,
/// gives this namespace its own record referencing the same content.
///
/// `false` means no namespace had it, or the one that did lost its content
/// to a concurrent DEL between the lookup and the clone -- both mean the
/// caller must store the bytes it holds.
async fn clone_from_another_namespace(
    storage: &Storage,
    namespace: &Namespace,
    key: &[u8],
) -> Result<bool> {
    let this = namespace.name();

    for source in storage.cas_namespaces()? {
        if source == this {
            continue;
        }
        // A point read per namespace. Deliberately not a store-wide content
        // index (ADR 0014): that would be a second reference-holding
        // structure with a lifecycle of its own, for a lookup that is
        // O(#cas namespaces) cheap reads.
        if !storage.get_namespace(&source)?.contains_key(key)? {
            continue;
        }

        let cloned = storage
            .cas()
            .clone_object_by_reference(&source, key, &this, key)
            .await?;
        if cloned.is_some() {
            debug!(
                key = %hex(key),
                source = %source,
                namespace = %this,
                "cas SET: the content is already in the store; referencing it"
            );
            return Ok(true);
        }
        debug!(
            key = %hex(key),
            source = %source,
            "cas SET: the record found in another namespace went away; storing the bytes"
        );
    }

    Ok(false)
}

/// A content address as tooling and logs print it.
pub(crate) fn hex(key: &[u8]) -> String {
    use std::fmt::Write;

    let mut out = String::with_capacity(key.len() * 2);
    for byte in key {
        let _ = write!(out, "{byte:02x}");
    }
    out
}
