use std::error::Error;
use std::fmt;
use std::path::Path;

use std::fmt::{Display, Formatter};

use super::store_header::StoreHeaderError;

/// Errors produced when decoding on-disk records.
///
/// Every variant carries enough detail to tell an operator *what* was
/// malformed, so a corrupted or foreign-format store surfaces as a described
/// error instead of a panic or a generic "corrupt object".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FsError {
    /// The record is shorter than the format requires.
    Truncated {
        record: &'static str,
        needed: usize,
        got: usize,
    },
    /// The record contains bytes past its self-described end.
    TrailingBytes { record: &'static str, extra: usize },
    /// A string field did not contain valid UTF-8.
    InvalidUtf8 {
        record: &'static str,
        field: &'static str,
    },
    /// The object type discriminant is not a known variant.
    UnknownObjectType(u8),
    /// A block-id width byte is not one of the supported widths.
    InvalidIdWidth(u8),
    /// A length or count field does not fit the platform's usize.
    LengthOverflow {
        record: &'static str,
        field: &'static str,
    },
}

impl Display for FsError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            FsError::Truncated {
                record,
                needed,
                got,
            } => write!(
                f,
                "Cas FS error: truncated {record} record: need at least {needed} bytes, got {got}"
            ),
            FsError::TrailingBytes { record, extra } => write!(
                f,
                "Cas FS error: {record} record has {extra} trailing bytes"
            ),
            FsError::InvalidUtf8 { record, field } => write!(
                f,
                "Cas FS error: {record} record field {field} is not valid UTF-8"
            ),
            FsError::UnknownObjectType(t) => {
                write!(f, "Cas FS error: unknown object type {t}")
            }
            FsError::InvalidIdWidth(w) => {
                write!(f, "Cas FS error: invalid block id width {w}")
            }
            FsError::LengthOverflow { record, field } => write!(
                f,
                "Cas FS error: {record} record field {field} does not fit in usize"
            ),
        }
    }
}

impl std::error::Error for FsError {}

// Define the error type
#[derive(Debug)]
pub enum MetaError {
    KeyNotFound,
    KeyAlreadyExists,
    CollectionNotFound,
    BucketNotFound,
    InsertError(String),
    RemoveError(String),
    NotMetaTree(String),
    TransactionError(String),
    PersistError(String),
    BlockNotFound,
    OtherDBError(String),
    /// A stored record failed to decode; carries the decode error.
    Corruption(FsError),
    /// The store at `path` may not be opened; carries the reason. This is the
    /// refusal an operator sees at startup, so it names both the store and
    /// what is wrong with its header.
    Header {
        path: String,
        source: StoreHeaderError,
    },
    /// A bucket name that the store reserves for itself was requested.
    ReservedBucketName(String),
    /// Another process already holds the store's lock.
    ///
    /// Routine rather than exceptional: fjall takes an exclusive lock on the
    /// database directory, so this is what an offline tool meets whenever the
    /// daemon is up. It used to be a panic; the whole point of the variant is
    /// that "someone else is using it" is an answer, not a crash.
    StoreLocked(String),
}

impl MetaError {
    /// Builds the refusal for a store whose header will not let it be opened.
    pub(crate) fn header(path: &Path, source: StoreHeaderError) -> Self {
        MetaError::Header {
            path: path.display().to_string(),
            source,
        }
    }
}

// Implement the std::error::Error trait
impl Error for MetaError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            MetaError::Corruption(e) => Some(e),
            MetaError::Header { source, .. } => Some(source),
            _ => None,
        }
    }
}

// Implement the Display trait for custom error messages
impl fmt::Display for MetaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            MetaError::KeyNotFound => write!(f, "Key not found"),
            MetaError::KeyAlreadyExists => write!(f, "Key already exists"),
            MetaError::CollectionNotFound => write!(f, "Collection not found"),
            MetaError::BucketNotFound => write!(f, "Bucket not found"),
            MetaError::InsertError(ref s) => write!(f, "Insert error: {s}"),
            MetaError::RemoveError(ref s) => write!(f, "Remove error: {s}"),
            MetaError::NotMetaTree(ref s) => write!(f, "Not a meta tree: {s}"),
            MetaError::TransactionError(ref s) => write!(f, "Transaction error: {s}"),
            MetaError::PersistError(ref s) => write!(f, "Persist error: {s}"),
            MetaError::BlockNotFound => write!(f, "Block not found"),
            MetaError::OtherDBError(ref s) => write!(f, "Other DB error: {s}"),
            MetaError::Corruption(ref e) => write!(f, "Corrupt record: {e}"),
            MetaError::Header {
                ref path,
                ref source,
            } => write!(f, "cannot open metadata store at {path}: {source}"),
            MetaError::ReservedBucketName(ref name) => write!(
                f,
                "bucket name \"{name}\" is reserved: names starting with '_' belong to the store's internal trees"
            ),
            MetaError::StoreLocked(ref path) => write!(
                f,
                "store at {path} is locked by another process (is the daemon running?)"
            ),
        }
    }
}

impl From<FsError> for MetaError {
    fn from(e: FsError) -> Self {
        MetaError::Corruption(e)
    }
}

use std::io;

impl From<MetaError> for io::Error {
    fn from(error: MetaError) -> Self {
        io::Error::other(error.to_string())
    }
}
