#[macro_use]
mod internal_macros;

pub mod api;
pub mod check;
pub mod inspect;
pub mod metrics;
pub mod retrieve;

// The one canonical path to the storage library: `s3cas::cas::*`, and
// `s3cas::cas::metastore::*` below it. One name, not three.
pub use cas_storage as cas;
