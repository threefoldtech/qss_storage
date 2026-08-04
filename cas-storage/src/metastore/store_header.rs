//! The QSST store header: the record that says "this directory is a store of
//! this format, addressed by this hash function".
//!
//! Every fjall database this codebase creates -- the shared block DB, every
//! namespace DB, respcas's key-value DB -- carries one 32 byte header in its own
//! `_STORE_HEADER` partition under a single fixed key. The header lives at the
//! `MetaStore` level rather than in the CAS layer so that a store which never
//! addresses a block (respcas's) still gets format versioning; the hash fields
//! are written everywhere and consulted only by `SharedBlockStore`.
//!
//! On-disk layout, little-endian, exactly [`STORE_HEADER_SIZE`] bytes:
//!
//! ```text
//! magic [4] = "QSST" | version u16 | hash_algo u8 | hash_width u8 |
//! created_at u64 (unix seconds) | store_id [16]
//! ```
//!
//! # The store id (ADR 0012)
//!
//! The last 16 bytes were the reserved block until ADR 0012 spent them on a
//! [`StoreId`]: the pairing identity that tells a blocks root which database
//! it belongs to. All-zero means *absent*, which is exactly what a header
//! written before ADR 0012 says -- so old stores are adopted at first open
//! rather than refused, and the format version does not move. A v4 UUID is
//! never all-zero, so the sentinel costs nothing.
//!
//! # Refusal, not migration
//!
//! A store whose header is missing, foreign, from a future version, or written
//! by a hash this build does not have is refused at open with an
//! operator-readable message. There is deliberately no fallback and no
//! migration: the blocks of a store are addressed by the hash named in its
//! header, so guessing wrong does not degrade the service, it corrupts it.
//!
//! # A version this build writes, and versions it opens
//!
//! [`STORE_HEADER_VERSION`] is what a store is CREATED at;
//! [`SUPPORTED_STORE_HEADER_VERSIONS`] is what it will open. The two differ
//! since ADR 0014, which raises a live store's version when a feature is
//! first used ([`raise_version`]) rather than at creation -- so a store that
//! never uses the feature stays openable by the builds that predate it, and
//! one that does is refused by them at the open instead of failing to decode
//! its own metadata later.
//!
//! # Forward compatibility
//!
//! The record has no spare bytes left: ADR 0012 spent the reserved block on
//! the store id. Whatever pattern the id bytes hold round-trips untouched, so
//! a build that only passes a header through neither rejects nor drops an id
//! it did not mint. Anything that changes the meaning of the existing fields,
//! or needs a field of its own, must bump `version`.

use std::fmt::{self, Display, Formatter};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::hasher::{Hasher, HasherError};

use super::FsError;
use super::codec::Reader;

/// Partition holding the store header. Named with the leading underscore that
/// marks every internal tree; bucket names starting with `_` are refused at
/// creation so a bucket can never collide with it.
pub const STORE_HEADER_TREE: &str = "_STORE_HEADER";

/// The single key under which the header record is stored.
pub const STORE_HEADER_KEY: &[u8] = b"header";

/// Serialized size of a header record. Fixed: a record of any other length is
/// corruption, not a variant.
pub const STORE_HEADER_SIZE: usize = 32;

/// Leading four bytes of every header record.
pub const STORE_HEADER_MAGIC: [u8; 4] = *b"QSST";

/// Format version a store is CREATED at.
///
/// v1: ADR 0002 -- BLAKE3 addressing, u64 on-disk fields, this header.
/// v2: ADR 0006 -- block records store a fanout depth instead of allocated
///     path bytes; the `_PATHS` tree no longer exists. A v1 store would be
///     misread (its block records do not decode as v2), so it is refused at
///     open. No migration exists: no deployed v1 store carries data.
/// v3: ADR 0005 -- block records gain a trailing flags byte carrying the
///     degraded bit. A v2 record is one byte short of a v3 record and fails
///     the exact-length check, so a v2 store is refused at open rather than
///     misread. Same stance as v2: no deployed store carries data.
/// v4: ADR 0014 -- a respcas store that holds a content-addressed namespace.
///     See [`STORE_HEADER_VERSION_CAS_NAMESPACE`]: this is the one version
///     that is RAISED on a live store rather than written at creation, so a
///     store keeps saying v3 until the feature is actually used.
pub const STORE_HEADER_VERSION: u16 = 3;

/// The version a store carries once it holds a respcas Cas namespace (ADR
/// 0014).
///
/// The namespace metadata of such a store contains a `key_mode` variant an
/// older build's msgpack decoder does not know, and a decode failure in the
/// middle of serving is not a refusal an operator can act on. So the store
/// says so in the one place every build reads first: the first `NSSET
/// key_mode cas` raises the header from [`STORE_HEADER_VERSION`] to this,
/// and an older build then refuses the open with
/// [`StoreHeaderError::UnsupportedVersion`] -- which is what the header is
/// for (ADR 0002).
///
/// Raised, never written at creation: a store that never uses the feature
/// stays readable by the builds that predate it.
pub const STORE_HEADER_VERSION_CAS_NAMESPACE: u16 = 4;

/// Every format version this build will open.
///
/// The version field is a gate, not a range to interpolate over: a store is
/// opened only if its exact version is listed here, and each entry has code
/// behind it that can read that store's records.
pub const SUPPORTED_STORE_HEADER_VERSIONS: &[u16] =
    &[STORE_HEADER_VERSION, STORE_HEADER_VERSION_CAS_NAMESPACE];

/// File name of the sidecar copy written in the store directory at creation.
/// Recovery from it is a manual operation.
///
/// The STORE directory, not the database directory, and the two are not
/// always the same: a respcas store from before ADR 0014 keeps its database
/// directly in the directory the operator named ([`write_sidecar`]), so the
/// sidecar of such a store sits beside fjall's own files rather than one
/// level up -- which is outside the store entirely.
pub const STORE_HEADER_SIDECAR: &str = "store_header.bin";

/// Width of the store id, in bytes: a UUID.
pub const STORE_ID_SIZE: usize = STORE_HEADER_SIZE - 4 - 2 - 1 - 1 - 8;

/// The pairing identity of a store (ADR 0012).
///
/// Sixteen bytes, minted once at store creation and never rewritten. Its job
/// is to answer one question at open: does this blocks root belong to this
/// database? The blocks root carries a copy in `blocks/.store-id`, and a
/// mismatch is refused rather than warned about -- a mispaired store is not a
/// degraded store, it is the wrong store.
///
/// All-zero is not a value: it is the on-disk spelling of "no id", which is
/// what every header written before ADR 0012 says. [`StoreId::from_bytes`] is
/// the only constructor from raw bytes and it rejects that pattern, so a
/// `StoreId` that exists is an id that was minted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StoreId([u8; STORE_ID_SIZE]);

impl StoreId {
    /// Mints a fresh id: a v4 UUID, so the entropy argument is someone
    /// else's and two stores created in the same second still differ.
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().into_bytes())
    }

    /// Wraps raw bytes, unless they are the all-zero "absent" pattern.
    pub fn from_bytes(bytes: [u8; STORE_ID_SIZE]) -> Option<Self> {
        if bytes == [0u8; STORE_ID_SIZE] {
            None
        } else {
            Some(Self(bytes))
        }
    }

    /// The id as it sits in the header.
    pub fn as_bytes(&self) -> &[u8; STORE_ID_SIZE] {
        &self.0
    }

    /// Lowercase hex, the form the `blocks/.store-id` marker carries.
    pub fn to_hex(&self) -> String {
        faster_hex::hex_string(&self.0)
    }

    /// Parses the marker's form: exactly [`STORE_ID_SIZE`] bytes of hex,
    /// surrounding whitespace tolerated (a marker an operator has echoed by
    /// hand ends with a newline).
    pub fn parse_hex(text: &str) -> Option<Self> {
        let trimmed = text.trim();
        if trimmed.len() != STORE_ID_SIZE * 2 {
            return None;
        }
        let mut bytes = [0u8; STORE_ID_SIZE];
        faster_hex::hex_decode(trimmed.as_bytes(), &mut bytes).ok()?;
        Self::from_bytes(bytes)
    }
}

impl Display for StoreId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// What a new store's header should say about its block hash.
///
/// Only consulted when a store is created; on open the header on disk wins,
/// because the blocks are already addressed by what it says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderSpec {
    /// Algorithm id as written to the header (see [`Hasher::algo_id`]).
    pub hash_algo: u8,
    /// Block address width in bytes as written to the header.
    pub hash_width: u8,
}

impl Default for HeaderSpec {
    /// BLAKE3 at the full 32 byte width: the default for new stores until the
    /// config file gives the choice a home.
    fn default() -> Self {
        Hasher::Blake3W32.into()
    }
}

impl From<Hasher> for HeaderSpec {
    fn from(hasher: Hasher) -> Self {
        Self {
            hash_algo: hasher.algo_id(),
            hash_width: hasher.width(),
        }
    }
}

/// A decoded, validated store header.
///
/// Validated by construction: the only ways to build one are
/// [`StoreHeader::create`] and [`StoreHeader::from_bytes`], and both refuse an
/// (algo, width) pair this build cannot turn into a [`Hasher`]. That is why
/// [`StoreHeader::hasher`] is total -- every value of this type names a hash
/// function that exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreHeader {
    version: u16,
    hasher: Hasher,
    created_at: u64,
    store_id: [u8; STORE_ID_SIZE],
}

impl StoreHeader {
    /// Builds the header for a store being created now, with a fresh
    /// [`StoreId`].
    ///
    /// `created_at` is seconds since the unix epoch, taken from the system
    /// clock; a clock set before 1970 records 0 rather than failing store
    /// creation over a timestamp that nothing but an operator reads.
    ///
    /// # Errors
    ///
    /// [`StoreHeaderError::Hash`] if `spec` does not name a hash this build
    /// has.
    pub fn create(spec: HeaderSpec) -> Result<Self, StoreHeaderError> {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        Self::create_at(spec, created_at)
    }

    /// [`StoreHeader::create`] with the creation timestamp supplied, so tests
    /// can pin one half of a golden vector.
    pub fn create_at(spec: HeaderSpec, created_at: u64) -> Result<Self, StoreHeaderError> {
        Self::create_with(spec, created_at, Some(StoreId::generate()))
    }

    /// The full constructor: timestamp and store id both supplied.
    ///
    /// `None` writes the absent pattern, which is what a pre-ADR-0012 header
    /// carries -- the shape adoption meets at open, and the one a golden
    /// vector can pin.
    pub fn create_with(
        spec: HeaderSpec,
        created_at: u64,
        store_id: Option<StoreId>,
    ) -> Result<Self, StoreHeaderError> {
        let hasher =
            Hasher::from_header(spec.hash_algo, spec.hash_width).map_err(StoreHeaderError::Hash)?;
        Ok(Self {
            version: STORE_HEADER_VERSION,
            hasher,
            created_at,
            store_id: store_id.map_or([0u8; STORE_ID_SIZE], |id| *id.as_bytes()),
        })
    }

    /// Format version of this header.
    pub fn version(&self) -> u16 {
        self.version
    }

    /// Algorithm id as written on disk.
    pub fn hash_algo(&self) -> u8 {
        self.hasher.algo_id()
    }

    /// Block address width in bytes as written on disk.
    pub fn hash_width(&self) -> u8 {
        self.hasher.width()
    }

    /// Store creation time, in seconds since the unix epoch.
    pub fn created_at(&self) -> u64 {
        self.created_at
    }

    /// The hash function this store addresses its blocks with.
    pub fn hasher(&self) -> Hasher {
        self.hasher
    }

    /// This store's pairing identity, or `None` for a header written before
    /// ADR 0012 -- the adoption case.
    pub fn store_id(&self) -> Option<StoreId> {
        StoreId::from_bytes(self.store_id)
    }

    /// The same header with `id` as its store identity: what adoption writes
    /// back. The header is a value, so this returns a new one rather than
    /// mutating the copy every opener is holding.
    #[must_use]
    pub fn with_store_id(self, id: StoreId) -> Self {
        Self {
            store_id: *id.as_bytes(),
            ..self
        }
    }

    /// Serializes the header to its exact on-disk form.
    ///
    /// The store id is written back as it was read, so a header this build
    /// only passed through survives the round trip intact -- including the
    /// absent pattern of a store that has not been adopted yet.
    pub fn to_bytes(&self) -> [u8; STORE_HEADER_SIZE] {
        let mut out = [0u8; STORE_HEADER_SIZE];
        out[..4].copy_from_slice(&STORE_HEADER_MAGIC);
        out[4..6].copy_from_slice(&self.version.to_le_bytes());
        out[6] = self.hash_algo();
        out[7] = self.hash_width();
        out[8..16].copy_from_slice(&self.created_at.to_le_bytes());
        out[16..].copy_from_slice(&self.store_id);
        out
    }

    /// Decodes and validates a header record.
    ///
    /// Checked in this order, because each answer only means something if the
    /// previous one held: the record is exactly [`STORE_HEADER_SIZE`] bytes,
    /// the magic is ours, the version is one we understand, and the (algo,
    /// width) pair names a hash this build has.
    pub fn from_bytes(buf: &[u8]) -> Result<Self, StoreHeaderError> {
        let mut reader = Reader::new("store header", buf);
        let magic: [u8; 4] = reader.array("magic").map_err(StoreHeaderError::Malformed)?;
        if magic != STORE_HEADER_MAGIC {
            return Err(StoreHeaderError::BadMagic(magic));
        }
        let version = reader.u16("version").map_err(StoreHeaderError::Malformed)?;
        if !SUPPORTED_STORE_HEADER_VERSIONS.contains(&version) {
            return Err(StoreHeaderError::UnsupportedVersion(version));
        }
        let hash_algo = reader
            .u8("hash_algo")
            .map_err(StoreHeaderError::Malformed)?;
        let hash_width = reader
            .u8("hash_width")
            .map_err(StoreHeaderError::Malformed)?;
        let created_at = reader
            .u64("created_at")
            .map_err(StoreHeaderError::Malformed)?;
        let store_id: [u8; STORE_ID_SIZE] = reader
            .array("store_id")
            .map_err(StoreHeaderError::Malformed)?;
        reader.finish().map_err(StoreHeaderError::Malformed)?;

        let hasher = Hasher::from_header(hash_algo, hash_width).map_err(StoreHeaderError::Hash)?;

        Ok(Self {
            version,
            hasher,
            created_at,
            store_id,
        })
    }
}

/// Why a store could not be opened as a QSST store.
///
/// The message text is the operator-facing contract: each variant names what
/// was found, so a refusal at startup says which of these five things went
/// wrong without a debugger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreHeaderError {
    /// The db directory holds data but no header record.
    Missing,
    /// The record does not start with `QSST`.
    BadMagic([u8; 4]),
    /// The format version is not the one this build writes.
    UnsupportedVersion(u16),
    /// The record is not a well-formed header of the right length.
    Malformed(FsError),
    /// The (algo, width) pair does not name a hash this build has.
    Hash(HasherError),
}

impl Display for StoreHeaderError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            StoreHeaderError::Missing => write!(
                f,
                "store predates the QSST format; no migration exists (expected a {STORE_HEADER_TREE} record)"
            ),
            StoreHeaderError::BadMagic(found) => write!(
                f,
                "not a QSST store: header magic is {} (\"{}\"), expected \"QSST\"",
                hex4(found),
                printable4(found)
            ),
            StoreHeaderError::UnsupportedVersion(version) => write!(
                f,
                "unsupported QSST store format version {version}; this build supports {}",
                SUPPORTED_STORE_HEADER_VERSIONS
                    .iter()
                    .map(u16::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            StoreHeaderError::Malformed(e) => {
                write!(f, "malformed QSST store header: {e}")
            }
            StoreHeaderError::Hash(e) => {
                write!(f, "store header names a hash this build cannot use: {e}")
            }
        }
    }
}

impl std::error::Error for StoreHeaderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StoreHeaderError::Malformed(e) => Some(e),
            StoreHeaderError::Hash(e) => Some(e),
            _ => None,
        }
    }
}

/// Renders four bytes as `0xaabbccdd`.
fn hex4(bytes: &[u8; 4]) -> String {
    format!(
        "0x{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3]
    )
}

/// Renders four bytes as ASCII, with `.` standing in for anything unprintable.
fn printable4(bytes: &[u8; 4]) -> String {
    bytes
        .iter()
        .map(|&b| {
            if b.is_ascii_graphic() || b == b' ' {
                b as char
            } else {
                '.'
            }
        })
        .collect()
}

mod ops;
#[cfg(test)]
mod tests;

pub use ops::{StoreInit, classify_db_dir, raise_version, read_header};
pub(crate) use ops::{adopt_store_id, write_header, write_sidecar};
