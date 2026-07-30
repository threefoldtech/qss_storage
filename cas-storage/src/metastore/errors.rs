use std::error::Error;
use std::fmt;

use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub enum FsError {
    MalformedObject,
}

impl Display for FsError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Cas FS error: {}",
            match self {
                FsError::MalformedObject => &"corrupt object",
            }
        )
    }
}

impl std::error::Error for FsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            &FsError::MalformedObject => None,
        }
    }
}

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
}

// Implement the std::error::Error trait
impl Error for MetaError {}

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
        }
    }
}

use std::io;

impl From<MetaError> for io::Error {
    fn from(error: MetaError) -> Self {
        io::Error::other(error.to_string())
    }
}
