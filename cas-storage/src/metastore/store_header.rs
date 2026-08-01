//! The QSST store header: the record that says "this directory is a store of
//! this format, addressed by this hash function".
//!
//! Every fjall database this codebase creates -- the shared block DB, every
//! namespace DB, respd's key-value DB -- carries one 32 byte header in its own
//! `_STORE_HEADER` partition under a single fixed key. The header lives at the
//! `MetaStore` level rather than in the CAS layer so that a store which never
//! addresses a block (respd's) still gets format versioning; the hash fields
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
//! # Forward compatibility
//!
//! The record has no spare bytes left: ADR 0012 spent the reserved block on
//! the store id. Whatever pattern the id bytes hold round-trips untouched, so
//! a build that only passes a header through neither rejects nor drops an id
//! it did not mint. Anything that changes the meaning of the existing fields,
//! or needs a field of its own, must bump `version`.

use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::hasher::{Hasher, HasherError};

use super::codec::Reader;
use super::{FsError, MetaError, Store};

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

/// Format version this build writes and is willing to open.
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
pub const STORE_HEADER_VERSION: u16 = 3;

/// File name of the sidecar copy written next to the db directory at
/// creation. Recovery from it is a manual operation.
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
        if version != STORE_HEADER_VERSION {
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
                "unsupported QSST store format version {version}; this build supports version {STORE_HEADER_VERSION}"
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

/// Whether a db directory is to be created or opened.
///
/// This decision has to be made *before* fjall touches the path, because
/// opening a fjall database creates the directory and its partitions -- after
/// that, "was there a store here?" can no longer be answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreInit {
    /// Nothing is there yet: write a fresh header.
    Create,
    /// Something is there: read its header and validate it.
    Open,
}

/// Decides [`StoreInit`] for `path`: a path that does not exist, or an empty
/// directory, means create; anything else means open.
pub fn classify_db_dir(path: &Path) -> Result<StoreInit, MetaError> {
    match std::fs::read_dir(path) {
        Ok(mut entries) => {
            if entries.next().is_none() {
                Ok(StoreInit::Create)
            } else {
                Ok(StoreInit::Open)
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(StoreInit::Create),
        Err(e) => Err(MetaError::OtherDBError(format!(
            "cannot inspect metadata store directory {}: {e}",
            path.display()
        ))),
    }
}

/// Reads the header record, if the store has one.
///
/// `db_path` is only used to name the store in an error message.
pub fn read_header(store: &dyn Store, db_path: &Path) -> Result<Option<StoreHeader>, MetaError> {
    let tree = store.tree_open(STORE_HEADER_TREE)?;
    match tree.get(STORE_HEADER_KEY)? {
        Some(raw) => StoreHeader::from_bytes(&raw)
            .map(Some)
            .map_err(|e| MetaError::header(db_path, e)),
        None => Ok(None),
    }
}

/// Writes the header record of a store being created.
///
/// No explicit persist call: this is an ordinary keyspace write, recovered
/// from fjall's journal like any other. It is the first write a new store
/// takes, so a crash cannot lose the header while keeping writes that came
/// after it.
pub(crate) fn write_header(store: &dyn Store, header: &StoreHeader) -> Result<(), MetaError> {
    let tree = store.tree_open(STORE_HEADER_TREE)?;
    tree.insert(STORE_HEADER_KEY, header.to_bytes().to_vec())
}

/// Writes the sidecar copy of the header next to the db directory.
///
/// Best effort on purpose: the sidecar is a backup for manual recovery, so a
/// store that is otherwise fine is not refused because this copy could not be
/// written. A failure is logged, loudly enough to notice.
pub(crate) fn write_sidecar(db_path: &Path, header: &StoreHeader) {
    let Some(parent) = db_path.parent() else {
        tracing::warn!(
            "no parent directory for {}: store header sidecar not written",
            db_path.display()
        );
        return;
    };
    let sidecar = parent.join(STORE_HEADER_SIDECAR);
    if let Err(e) = std::fs::write(&sidecar, header.to_bytes()) {
        tracing::warn!(
            "could not write store header sidecar {}: {e}",
            sidecar.display()
        );
    }
}

/// Records `id` in an existing store's header, and returns the header as it
/// now reads (ADR 0012 adoption).
///
/// This is the FIRST of the two adoption writes: the header side is the
/// authoritative one, so it lands before the blocks root's marker. A crash
/// between them leaves a header with an id and a root without one, which the
/// next open completes by writing the marker -- the direction that needs no
/// judgement.
///
/// The sidecar is rewritten too, so the manual-recovery copy never claims a
/// different identity than the record it copies.
pub(crate) fn adopt_store_id(
    store: &dyn Store,
    db_path: &Path,
    header: StoreHeader,
    id: StoreId,
) -> Result<StoreHeader, MetaError> {
    let adopted = header.with_store_id(id);
    write_header(store, &adopted)?;
    write_sidecar(db_path, &adopted);
    Ok(adopted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::{FjallStore, MetaStore};
    use std::path::PathBuf;
    use tempfile::{TempDir, tempdir};

    /// A header created with the default spec at a pinned timestamp and no
    /// store id, byte for byte -- the shape every pre-ADR-0012 store has on
    /// disk. Changing this vector changes the on-disk format.
    ///
    /// magic "QSST" | version 3 | algo 1 (blake3) | width 32 |
    /// created_at 0x0000000068000001 | 16 zero bytes (store id absent)
    const GOLDEN: [u8; STORE_HEADER_SIZE] = [
        0x51, 0x53, 0x53, 0x54, // "QSST"
        0x03, 0x00, // version 3 (ADR 0005 block record flags byte)
        0x01, // algo: blake3
        0x20, // width: 32
        0x01, 0x00, 0x00, 0x68, 0x00, 0x00, 0x00, 0x00, // created_at
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // store id: absent
    ];

    const GOLDEN_CREATED_AT: u64 = 0x0000_0000_6800_0001;

    #[test]
    fn golden_vector() {
        let header =
            StoreHeader::create_with(HeaderSpec::default(), GOLDEN_CREATED_AT, None).unwrap();
        assert_eq!(header.to_bytes(), GOLDEN);

        let decoded = StoreHeader::from_bytes(&GOLDEN).unwrap();
        assert_eq!(decoded.version(), STORE_HEADER_VERSION);
        assert_eq!(decoded.hash_algo(), 1);
        assert_eq!(decoded.hash_width(), 32);
        assert_eq!(decoded.created_at(), GOLDEN_CREATED_AT);
        assert_eq!(decoded.hasher(), Hasher::Blake3W32);
        assert_eq!(decoded.store_id(), None, "an all-zero id is no id");
        assert_eq!(decoded, header);
    }

    /// The other half of the vector: the same header with an id in it. The
    /// id occupies the last 16 bytes and nothing else moves.
    #[test]
    fn golden_vector_with_a_store_id() {
        let id = StoreId::from_bytes([
            0x0f, 0x1e, 0x2d, 0x3c, 0x4b, 0x5a, 0x69, 0x78, 0x87, 0x96, 0xa5, 0xb4, 0xc3, 0xd2,
            0xe1, 0xf0,
        ])
        .unwrap();
        let header =
            StoreHeader::create_with(HeaderSpec::default(), GOLDEN_CREATED_AT, Some(id)).unwrap();

        let mut expected = GOLDEN;
        expected[16..].copy_from_slice(id.as_bytes());
        assert_eq!(header.to_bytes(), expected);
        assert_eq!(header.store_id(), Some(id));
        assert_eq!(id.to_hex(), "0f1e2d3c4b5a69788796a5b4c3d2e1f0");
        assert_eq!(StoreId::parse_hex(&format!("{id}\n")), Some(id));
        assert_eq!(StoreHeader::from_bytes(&expected).unwrap(), header);
    }

    /// The absent pattern is not a value, and neither is a truncated or
    /// non-hex marker: all three are "this root claims nothing".
    #[test]
    fn store_ids_reject_the_absent_pattern_and_junk() {
        assert_eq!(StoreId::from_bytes([0u8; STORE_ID_SIZE]), None);
        assert_eq!(StoreId::parse_hex(&"0".repeat(32)), None);
        assert_eq!(StoreId::parse_hex(""), None);
        assert_eq!(StoreId::parse_hex("deadbeef"), None);
        assert_eq!(StoreId::parse_hex(&"z".repeat(32)), None);
        assert_ne!(StoreId::generate(), StoreId::generate());
        let id = StoreId::generate();
        assert_eq!(StoreId::parse_hex(&id.to_hex()), Some(id));
    }

    /// A created store gets an id; adoption is only for stores that predate
    /// the field.
    #[test]
    fn created_headers_carry_a_fresh_id() {
        let first = StoreHeader::create(HeaderSpec::default()).unwrap();
        let second = StoreHeader::create(HeaderSpec::default()).unwrap();
        assert!(first.store_id().is_some());
        assert_ne!(first.store_id(), second.store_id());
    }

    #[test]
    fn round_trips_both_widths() {
        for hasher in [Hasher::Blake3W16, Hasher::Blake3W32] {
            let header = StoreHeader::create_at(hasher.into(), 42).unwrap();
            let bytes = header.to_bytes();
            assert_eq!(bytes.len(), STORE_HEADER_SIZE);
            assert_eq!(StoreHeader::from_bytes(&bytes).unwrap(), header);
            assert_eq!(header.hasher(), hasher);
        }
    }

    /// Whatever is in the id bytes round-trips: a build that only passes a
    /// header through must neither reject an id it did not mint nor drop it.
    #[test]
    fn store_id_bytes_round_trip_untouched() {
        let mut raw = GOLDEN;
        raw[16..].copy_from_slice(&[0xabu8; STORE_ID_SIZE]);

        let header = StoreHeader::from_bytes(&raw).expect("any id pattern must be accepted");
        assert_eq!(header.to_bytes(), raw);
        assert_eq!(header.store_id().unwrap().as_bytes(), &[0xabu8; 16]);
    }

    /// Adoption changes the id and nothing else.
    #[test]
    fn with_store_id_touches_only_the_id() {
        let before = StoreHeader::from_bytes(&GOLDEN).unwrap();
        let id = StoreId::generate();
        let after = before.with_store_id(id);

        assert_eq!(after.store_id(), Some(id));
        assert_eq!(after.version(), before.version());
        assert_eq!(after.hasher(), before.hasher());
        assert_eq!(after.created_at(), before.created_at());
        assert_eq!(&after.to_bytes()[..16], &before.to_bytes()[..16]);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut raw = GOLDEN;
        raw[..4].copy_from_slice(b"JUNK");
        let err = StoreHeader::from_bytes(&raw).unwrap_err();
        assert_eq!(err, StoreHeaderError::BadMagic(*b"JUNK"));
        let msg = err.to_string();
        assert!(msg.contains("0x4a554e4b"), "{msg}");
        assert!(msg.contains("\"JUNK\""), "{msg}");
        assert!(msg.contains("expected \"QSST\""), "{msg}");
    }

    #[test]
    fn unprintable_magic_is_still_named() {
        let mut raw = GOLDEN;
        raw[..4].copy_from_slice(&[0x00, 0xff, 0x41, 0x0a]);
        let msg = StoreHeader::from_bytes(&raw).unwrap_err().to_string();
        assert!(msg.contains("0x00ff410a"), "{msg}");
        assert!(msg.contains("\"..A.\""), "{msg}");
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut raw = GOLDEN;
        raw[4..6].copy_from_slice(&4u16.to_le_bytes());
        let err = StoreHeader::from_bytes(&raw).unwrap_err();
        assert_eq!(err, StoreHeaderError::UnsupportedVersion(4));
        assert!(
            err.to_string()
                .contains("unsupported QSST store format version 4"),
            "{err}"
        );
    }

    /// The migration gate for ADR 0005's block record change: a v2 store
    /// must be refused at open, not misread.
    #[test]
    fn rejects_the_previous_version() {
        let mut raw = GOLDEN;
        raw[4..6].copy_from_slice(&2u16.to_le_bytes());
        let err = StoreHeader::from_bytes(&raw).unwrap_err();
        assert_eq!(err, StoreHeaderError::UnsupportedVersion(2));
    }

    #[test]
    fn rejects_unknown_algo_and_width() {
        let mut raw = GOLDEN;
        raw[6] = 9;
        let err = StoreHeader::from_bytes(&raw).unwrap_err();
        assert_eq!(err, StoreHeaderError::Hash(HasherError::UnknownAlgo(9)));
        assert!(
            err.to_string()
                .contains("unknown block hash algorithm id 9"),
            "{err}"
        );

        let mut raw = GOLDEN;
        raw[7] = 17;
        let err = StoreHeader::from_bytes(&raw).unwrap_err();
        assert_eq!(
            err,
            StoreHeaderError::Hash(HasherError::UnsupportedWidth(17))
        );
        assert!(
            err.to_string().contains("unsupported block hash width 17"),
            "{err}"
        );
    }

    #[test]
    fn rejects_wrong_length() {
        let short = StoreHeader::from_bytes(&GOLDEN[..31]).unwrap_err();
        assert!(
            matches!(
                short,
                StoreHeaderError::Malformed(FsError::Truncated { .. })
            ),
            "{short:?}"
        );

        let mut long = GOLDEN.to_vec();
        long.push(0);
        let long = StoreHeader::from_bytes(&long).unwrap_err();
        assert!(
            matches!(
                long,
                StoreHeaderError::Malformed(FsError::TrailingBytes { .. })
            ),
            "{long:?}"
        );
    }

    // ---- store-level tests ----

    fn fjall(path: PathBuf) -> Result<FjallStore, MetaError> {
        FjallStore::new(path, Some(1), None)
    }

    /// Creating a store writes a header; reopening the same directory reads
    /// exactly that header back.
    fn create_then_reopen<S: Store + 'static>(build: impl Fn(PathBuf) -> Result<S, MetaError>) {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db");

        let created = {
            let (_store, header) =
                MetaStore::open_or_create(db.clone(), Some(1), HeaderSpec::default(), &build)
                    .expect("fresh directory must be created");
            header
        };
        assert_eq!(created.hasher(), Hasher::Blake3W32);

        let (_store, reopened) =
            MetaStore::open_or_create(db.clone(), Some(1), HeaderSpec::default(), &build)
                .expect("a store we just created must reopen");
        assert_eq!(reopened, created);
    }

    #[test]
    fn create_then_reopen_fjall() {
        create_then_reopen(fjall);
    }

    /// The width is taken from the header on open, not from the spec the
    /// caller happens to pass.
    #[test]
    fn header_wins_over_spec_on_open() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db");

        // The first store has to be dropped before the second opens: fjall
        // holds a lock on the directory for as long as the database lives.
        let created = {
            let (_store, created) =
                MetaStore::open_or_create(db.clone(), Some(1), Hasher::Blake3W16.into(), fjall)
                    .unwrap();
            created
        };
        assert_eq!(created.hasher(), Hasher::Blake3W16);

        let (_store, reopened) =
            MetaStore::open_or_create(db.clone(), Some(1), Hasher::Blake3W32.into(), fjall)
                .unwrap();
        assert_eq!(reopened.hasher(), Hasher::Blake3W16);
    }

    #[test]
    fn creation_writes_the_sidecar() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db");
        assert!(!db.exists(), "the create path starts from a missing dir");

        let (_store, header) =
            MetaStore::open_or_create(db.clone(), Some(1), HeaderSpec::default(), fjall).unwrap();

        let sidecar = dir.path().join(STORE_HEADER_SIDECAR);
        let bytes = std::fs::read(&sidecar).expect("sidecar must exist next to the db dir");
        assert_eq!(bytes, header.to_bytes());
    }

    /// Adoption is durable on both copies: the record a reopen reads and the
    /// sidecar a manual recovery reads say the same id.
    #[test]
    fn adoption_writes_the_header_record_and_the_sidecar() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db");
        let adopted = StoreId::generate();

        let created = {
            let (meta, header) =
                MetaStore::open_or_create(db.clone(), Some(1), HeaderSpec::default(), fjall)
                    .unwrap();
            assert!(header.store_id().is_some(), "a new store mints its own");
            adopt_store_id(&*meta.get_underlying_store(), &db, header, adopted).unwrap()
        };
        assert_eq!(created.store_id(), Some(adopted));

        let (_meta, reopened) =
            MetaStore::open_or_create(db.clone(), Some(1), HeaderSpec::default(), fjall).unwrap();
        assert_eq!(reopened.store_id(), Some(adopted));

        let sidecar = std::fs::read(dir.path().join(STORE_HEADER_SIDECAR)).unwrap();
        assert_eq!(sidecar, created.to_bytes());
    }

    /// Builds a store the raw way (no header), leaving a non-empty db
    /// directory behind, and returns the path it lives at.
    fn unheadered_store(dir: &TempDir) -> PathBuf {
        let db = dir.path().join("db");
        let store = fjall(db.clone()).unwrap();
        // Give the store some content, so that it is a real pre-QSST store and
        // not just an empty directory.
        store
            .tree_open("bucket")
            .unwrap()
            .insert(b"key", b"value".to_vec())
            .unwrap();
        drop(store);
        db
    }

    #[test]
    fn refuses_a_store_that_predates_the_header() {
        let dir = tempdir().unwrap();
        let db = unheadered_store(&dir);

        let err = MetaStore::open_or_create(db, Some(1), HeaderSpec::default(), fjall).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("store predates the QSST format; no migration exists"),
            "{msg}"
        );
    }

    /// Overwrites the header record of an existing store with `raw`, the way a
    /// corrupted or foreign header would look on disk.
    fn doctor_header(db: &Path, raw: Vec<u8>) {
        let store = fjall(db.to_path_buf()).unwrap();
        store
            .tree_open(STORE_HEADER_TREE)
            .unwrap()
            .insert(STORE_HEADER_KEY, raw)
            .unwrap();
        drop(store);
    }

    /// Creates a healthy store, doctors its header with `raw`, and returns the
    /// error text produced by reopening it.
    fn refusal_for(raw: Vec<u8>) -> String {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db");
        {
            let _ = MetaStore::open_or_create(db.clone(), Some(1), HeaderSpec::default(), fjall)
                .unwrap();
        }
        doctor_header(&db, raw);

        MetaStore::open_or_create(db, Some(1), HeaderSpec::default(), fjall)
            .expect_err("a doctored header must be refused")
            .to_string()
    }

    #[test]
    fn refuses_a_doctored_magic() {
        let mut raw = GOLDEN;
        raw[..4].copy_from_slice(b"SLED");
        let msg = refusal_for(raw.to_vec());
        assert!(msg.contains("not a QSST store"), "{msg}");
        assert!(msg.contains("\"SLED\""), "{msg}");
    }

    #[test]
    fn refuses_a_doctored_version() {
        let mut raw = GOLDEN;
        raw[4..6].copy_from_slice(&4u16.to_le_bytes());
        let msg = refusal_for(raw.to_vec());
        assert!(
            msg.contains("unsupported QSST store format version 4"),
            "{msg}"
        );
    }

    #[test]
    fn refuses_a_doctored_algo() {
        let mut raw = GOLDEN;
        raw[6] = 9;
        let msg = refusal_for(raw.to_vec());
        assert!(msg.contains("unknown block hash algorithm id 9"), "{msg}");
    }

    #[test]
    fn refuses_a_doctored_width() {
        let mut raw = GOLDEN;
        raw[7] = 17;
        let msg = refusal_for(raw.to_vec());
        assert!(msg.contains("unsupported block hash width 17"), "{msg}");
    }

    #[test]
    fn refuses_a_truncated_header_record() {
        let msg = refusal_for(GOLDEN[..16].to_vec());
        assert!(msg.contains("malformed QSST store header"), "{msg}");
    }

    /// Every refusal names the store it is about, so an operator running
    /// several stores knows which one to look at.
    #[test]
    fn refusals_name_the_store_path() {
        let dir = tempdir().unwrap();
        let db = unheadered_store(&dir);
        let msg = MetaStore::open_or_create(db.clone(), Some(1), HeaderSpec::default(), fjall)
            .unwrap_err()
            .to_string();
        assert!(msg.contains(&db.display().to_string()), "{msg}");
    }
}
