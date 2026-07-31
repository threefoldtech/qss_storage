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
//! created_at u64 (unix seconds) | reserved [16]
//! ```
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
//! The 16 reserved bytes are written zeroed and are *not* rejected when a
//! future version puts something there -- they round-trip untouched. Anything
//! that changes the meaning of the existing fields must bump `version`
//! instead.

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
pub const STORE_HEADER_VERSION: u16 = 1;

/// File name of the sidecar copy written next to the db directory at
/// creation. Recovery from it is a manual operation.
pub const STORE_HEADER_SIDECAR: &str = "store_header.bin";

/// Number of trailing bytes reserved for future fields.
const RESERVED_SIZE: usize = STORE_HEADER_SIZE - 4 - 2 - 1 - 1 - 8;

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
    reserved: [u8; RESERVED_SIZE],
}

impl StoreHeader {
    /// Builds the header for a store being created now.
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
    /// can pin a golden vector.
    pub fn create_at(spec: HeaderSpec, created_at: u64) -> Result<Self, StoreHeaderError> {
        let hasher =
            Hasher::from_header(spec.hash_algo, spec.hash_width).map_err(StoreHeaderError::Hash)?;
        Ok(Self {
            version: STORE_HEADER_VERSION,
            hasher,
            created_at,
            reserved: [0u8; RESERVED_SIZE],
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

    /// Serializes the header to its exact on-disk form.
    ///
    /// The reserved bytes are written back as they were read, so a header
    /// written by a future version survives a read-write cycle intact. Every
    /// header *this* build creates has them zeroed.
    pub fn to_bytes(&self) -> [u8; STORE_HEADER_SIZE] {
        let mut out = [0u8; STORE_HEADER_SIZE];
        out[..4].copy_from_slice(&STORE_HEADER_MAGIC);
        out[4..6].copy_from_slice(&self.version.to_le_bytes());
        out[6] = self.hash_algo();
        out[7] = self.hash_width();
        out[8..16].copy_from_slice(&self.created_at.to_le_bytes());
        out[16..].copy_from_slice(&self.reserved);
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
        let reserved: [u8; RESERVED_SIZE] = reader
            .array("reserved")
            .map_err(StoreHeaderError::Malformed)?;
        reader.finish().map_err(StoreHeaderError::Malformed)?;

        let hasher = Hasher::from_header(hash_algo, hash_width).map_err(StoreHeaderError::Hash)?;

        Ok(Self {
            version,
            hasher,
            created_at,
            reserved,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::{FjallStore, MetaStore};
    use std::path::PathBuf;
    use tempfile::{TempDir, tempdir};

    /// A header created with the default spec at a pinned timestamp, byte for
    /// byte. Changing this vector changes the on-disk format.
    ///
    /// magic "QSST" | version 1 | algo 1 (blake3) | width 32 |
    /// created_at 0x0000000068000001 | 16 zero bytes
    const GOLDEN: [u8; STORE_HEADER_SIZE] = [
        0x51, 0x53, 0x53, 0x54, // "QSST"
        0x01, 0x00, // version 1
        0x01, // algo: blake3
        0x20, // width: 32
        0x01, 0x00, 0x00, 0x68, 0x00, 0x00, 0x00, 0x00, // created_at
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // reserved
    ];

    const GOLDEN_CREATED_AT: u64 = 0x0000_0000_6800_0001;

    #[test]
    fn golden_vector() {
        let header = StoreHeader::create_at(HeaderSpec::default(), GOLDEN_CREATED_AT).unwrap();
        assert_eq!(header.to_bytes(), GOLDEN);

        let decoded = StoreHeader::from_bytes(&GOLDEN).unwrap();
        assert_eq!(decoded.version(), STORE_HEADER_VERSION);
        assert_eq!(decoded.hash_algo(), 1);
        assert_eq!(decoded.hash_width(), 32);
        assert_eq!(decoded.created_at(), GOLDEN_CREATED_AT);
        assert_eq!(decoded.hasher(), Hasher::Blake3W32);
        assert_eq!(decoded, header);
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

    /// A future version may use the reserved bytes; this build must neither
    /// reject them nor drop them.
    #[test]
    fn reserved_bytes_round_trip_untouched() {
        let mut raw = GOLDEN;
        raw[16..].copy_from_slice(&[0xabu8; RESERVED_SIZE]);

        let header = StoreHeader::from_bytes(&raw).expect("nonzero reserved must be accepted");
        assert_eq!(header.to_bytes(), raw);
    }

    #[test]
    fn created_headers_zero_the_reserved_bytes() {
        let bytes = StoreHeader::create(HeaderSpec::default())
            .unwrap()
            .to_bytes();
        assert_eq!(&bytes[16..], &[0u8; RESERVED_SIZE]);
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
        raw[4..6].copy_from_slice(&2u16.to_le_bytes());
        let err = StoreHeader::from_bytes(&raw).unwrap_err();
        assert_eq!(err, StoreHeaderError::UnsupportedVersion(2));
        assert!(
            err.to_string()
                .contains("unsupported QSST store format version 2"),
            "{err}"
        );
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

    fn fjall(path: PathBuf) -> FjallStore {
        FjallStore::new(path, Some(1), None)
    }

    /// Creating a store writes a header; reopening the same directory reads
    /// exactly that header back.
    fn create_then_reopen<S: Store + 'static>(build: impl Fn(PathBuf) -> S) {
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

    /// Builds a store the raw way (no header), leaving a non-empty db
    /// directory behind, and returns the path it lives at.
    fn unheadered_store(dir: &TempDir) -> PathBuf {
        let db = dir.path().join("db");
        let store = fjall(db.clone());
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
        let store = fjall(db.to_path_buf());
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
        raw[4..6].copy_from_slice(&2u16.to_le_bytes());
        let msg = refusal_for(raw.to_vec());
        assert!(
            msg.contains("unsupported QSST store format version 2"),
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
