use std::fs;
use std::io;
use std::path::Path;
use std::sync::atomic::Ordering;

use crate::metastore::StoreId;

use super::{BlockDiskOps, RealDiskOps, STORE_ID_MARKER_NAME, TEMP_NONCE, TMP_DIR_NAME};

/// What the blocks root's [`STORE_ID_MARKER_NAME`] file says, if anything
/// (ADR 0012).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoreIdMarker {
    /// No marker file. Either a store from before ADR 0012, or a root whose
    /// adoption was interrupted between the header write and this one.
    Absent,
    /// A marker that is not a store id: truncated, empty, not hex. Damage
    /// rather than a claim -- it names no store, so nothing can be compared
    /// against it and the header's id is written over it.
    Unreadable(String),
    /// The store this blocks root says it belongs to.
    Present(StoreId),
}

/// Reads the blocks root's pairing marker.
///
/// A missing file is [`StoreIdMarker::Absent`], not an error: that is the
/// legacy and the half-adopted shape. Anything present but unparseable is
/// [`StoreIdMarker::Unreadable`], carrying the bytes as the operator would
/// see them (lossily, truncated) so a log line can show what was there.
///
/// # Errors
///
/// Only a real IO failure -- unreadable directory, EIO -- which must not be
/// mistaken for "this root claims nothing".
pub(crate) fn read_store_id_marker(blocks_root: &Path) -> io::Result<StoreIdMarker> {
    let path = blocks_root.join(STORE_ID_MARKER_NAME);
    match fs::read(&path) {
        Ok(raw) => {
            let text = String::from_utf8_lossy(&raw);
            Ok(match StoreId::parse_hex(&text) {
                Some(id) => StoreIdMarker::Present(id),
                None => StoreIdMarker::Unreadable(
                    text.chars()
                        .take(64)
                        .collect::<String>()
                        .replace(|c: char| c.is_control(), "."),
                ),
            })
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(StoreIdMarker::Absent),
        Err(e) => Err(e),
    }
}

/// Writes the pairing marker by the block protocol's own rules: exclusive
/// create in `.tmp`, fsync, rename into the blocks root, fsync the root.
///
/// The temp file shares the blocks root's filesystem (the same-device check
/// at open guarantees it), so the rename is atomic and a reader never sees a
/// half-written identity. Synced whatever the durability level says: this is
/// one write per store lifetime, and a marker that did not survive the crash
/// that adopted it would be re-adopted from scratch on a root that may have
/// changed hands in between.
pub(super) fn write_marker(
    ops: &dyn BlockDiskOps,
    root: &Path,
    tmp: &Path,
    id: StoreId,
) -> io::Result<()> {
    ops.create_dir_all(tmp)?;
    let nonce = TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
    let temp_path = tmp.join(format!("{STORE_ID_MARKER_NAME}-{nonce}"));
    let contents = format!("{}\n", id.to_hex());

    let attempt = (|| -> io::Result<()> {
        ops.write_new_file(&temp_path, contents.as_bytes())?;
        ops.fsync_file(&temp_path, false)?;
        ops.rename(&temp_path, &root.join(STORE_ID_MARKER_NAME))?;
        ops.fsync_dir(root)
    })();

    if let Err(e) = attempt {
        // Best effort: the temp is garbage either way and the open-time
        // purge is the backstop.
        let _ = ops.remove_file(&temp_path);
        return Err(e);
    }
    Ok(())
}

/// [`write_marker`] against the real filesystem, for callers that hold a
/// blocks root but no writer -- fsck's re-pair verb.
pub(crate) fn rewrite_store_id_marker(blocks_root: &Path, id: StoreId) -> io::Result<()> {
    write_marker(
        &RealDiskOps,
        blocks_root,
        &blocks_root.join(TMP_DIR_NAME),
        id,
    )
}
