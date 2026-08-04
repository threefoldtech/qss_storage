use super::{
    STORE_HEADER_KEY, STORE_HEADER_SIDECAR, STORE_HEADER_TREE, SUPPORTED_STORE_HEADER_VERSIONS,
    StoreHeader, StoreHeaderError, StoreId,
};
use crate::metastore::{MetaError, Store};
use std::path::Path;

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

/// Writes the sidecar copy of the header into `store_dir`.
///
/// `store_dir` is the STORE directory -- the one an operator named -- and is
/// passed in rather than derived from the database path, because the two
/// layouts this workspace opens put the database in different places
/// relative to it:
///
/// ```text
/// <store>/db      the database of every store this build creates -> <store>/
/// <store>         a respcas store from before ADR 0014            -> <store>/
/// ```
///
/// Deriving it as the database's parent is right for the first and wrong for
/// the second, where it names the store's own parent: raising such a store's
/// version wrote a `store_header.bin` OUTSIDE the store and left the copy
/// inside it stale. Only the caller knows which directory it opened, so only
/// the caller can say.
///
/// Best effort on purpose: the sidecar is a backup for manual recovery, so a
/// store that is otherwise fine is not refused because this copy could not be
/// written. A failure is logged, loudly enough to notice.
pub(crate) fn write_sidecar(store_dir: &Path, header: &StoreHeader) {
    let sidecar = store_dir.join(STORE_HEADER_SIDECAR);
    if let Err(e) = std::fs::write(&sidecar, header.to_bytes()) {
        tracing::warn!(
            "could not write store header sidecar {}: {e}",
            sidecar.display()
        );
    }
}

/// Records `id` in an existing store's header, and returns the header as it
/// now reads (ADR 0012 adoption).
///
/// This is the FIRST of the two adoption writes: the header side is the
/// authoritative one, so it lands before the blocks root's marker. A crash
/// between them leaves a header with an id and a root without one, which the
/// next open completes by writing the marker -- the direction that needs no
/// judgement.
///
/// The sidecar is rewritten too, so the manual-recovery copy never claims a
/// different identity than the record it copies. `store_dir` says where that
/// copy belongs; see [`write_sidecar`].
pub(crate) fn adopt_store_id(
    store: &dyn Store,
    store_dir: &Path,
    header: StoreHeader,
    id: StoreId,
) -> Result<StoreHeader, MetaError> {
    let adopted = header.with_store_id(id);
    write_header(store, &adopted)?;
    write_sidecar(store_dir, &adopted);
    Ok(adopted)
}

/// Raises an existing store's format version to `version`, and returns the
/// header as it now reads.
///
/// The one write that changes a store's version after creation, and it goes
/// in one direction only: a store already at or above `version` is left
/// alone, so calling this on every use of the feature that needs it costs a
/// point read and nothing else.
///
/// Its caller is respcas creating the first Cas namespace in a store (ADR
/// 0014). What the raise BUYS is the refusal an older build gives afterwards
/// -- it must therefore land before the metadata that older build cannot
/// decode, never after.
///
/// `store_dir` is where the sidecar copy belongs, which respcas knows and
/// this function cannot derive: on the pre-0014 layout the database IS the
/// store directory (see [`write_sidecar`]).
///
/// # Errors
///
/// [`MetaError::Header`] if `version` is not one this build supports (a build
/// may only raise a store to a version it can itself open), or if the store
/// has no header to raise.
pub fn raise_version(
    store: &dyn Store,
    db_path: &Path,
    store_dir: &Path,
    version: u16,
) -> Result<StoreHeader, MetaError> {
    if !SUPPORTED_STORE_HEADER_VERSIONS.contains(&version) {
        return Err(MetaError::header(
            db_path,
            StoreHeaderError::UnsupportedVersion(version),
        ));
    }
    let header = read_header(store, db_path)?
        .ok_or_else(|| MetaError::header(db_path, StoreHeaderError::Missing))?;
    if header.version >= version {
        return Ok(header);
    }
    let raised = StoreHeader { version, ..header };
    write_header(store, &raised)?;
    write_sidecar(store_dir, &raised);
    tracing::info!(
        "raised the QSST store format version of {} from {} to {version}",
        db_path.display(),
        header.version,
    );
    Ok(raised)
}
