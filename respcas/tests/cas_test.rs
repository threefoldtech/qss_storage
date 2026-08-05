//! Content-addressed namespaces, from the wire (ADR 0014).

mod common;

#[path = "cas_test/helpers.rs"]
mod helpers;

#[path = "cas_test/dedup.rs"]
mod dedup;
#[path = "cas_test/ingest.rs"]
mod ingest;
#[path = "cas_test/key_mode.rs"]
mod key_mode;
#[path = "cas_test/scan.rs"]
mod scan;
