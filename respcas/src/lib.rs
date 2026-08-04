//! Redis-compatible server (respcas) using metastore as backend

// The binary declares its own module tree in main.rs and never goes through
// this crate, so what stays public below is test access, not an API: it is the
// set of items the integration tests in tests/ drive directly, and nothing
// else.
pub mod cmd;
mod conn;
pub mod content;
pub mod namespace;
mod property;
mod resp;
pub mod server;
pub mod storage;
