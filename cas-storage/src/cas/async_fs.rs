//! Synchronous seam for the disk write path.
//!
//! The trait exists so the on-disk write can be mocked in tests (see
//! `test_store_object_write_failure`). It is intentionally synchronous:
//! the methods were made sync in c5f9cc9 to fix the Fjall deadlock
//! (see docs/arch/deadlock-fix.md), and `async_trait` was dead
//! decoration ever since and has been stripped per ADR-004.

pub(super) trait AsyncFileSystem: Send + Sync + std::fmt::Debug {
    fn create_dir_all(&self, path: &std::path::Path) -> std::io::Result<()>;
    fn write(&self, path: &std::path::Path, contents: &[u8]) -> std::io::Result<()>;
}

#[derive(Debug)]
pub(super) struct RealAsyncFs;

impl AsyncFileSystem for RealAsyncFs {
    fn create_dir_all(&self, path: &std::path::Path) -> std::io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn write(&self, path: &std::path::Path, contents: &[u8]) -> std::io::Result<()> {
        std::fs::write(path, contents)
    }
}
