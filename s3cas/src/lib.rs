#[macro_use]
mod internal_macros;

pub mod check;
pub mod inspect;
pub mod metrics;
pub mod retrieve;
pub mod s3fs;

// Re-export cas-storage so downstream code can use `s3cas::cas_storage::*`
// or `s3cas::cas::*` / `s3cas::metastore::*` as before.
pub use cas_storage;
pub use cas_storage as cas;
pub use cas_storage::metastore;
