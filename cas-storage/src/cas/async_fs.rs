//! Synchronous seam for the disk write path.
//!
//! The trait exists so the on-disk write can be mocked in tests (see
//! `test_store_object_write_failure`). It is intentionally synchronous:
//! an async write awaited while fjall's single-writer lock was held could
//! park every executor thread on that lock -- see
//! docs/arch/deadlock-fix.md for the full account. The `async_trait`
//! decoration that remained after the sync conversion was dead weight and
//! has been stripped.

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
