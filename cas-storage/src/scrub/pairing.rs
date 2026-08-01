//! The pairing identity, seen from the tool side (ADR 0012).
//!
//! Two things live here, and the difference between them is the whole point:
//!
//! - [`read_pairing`]: what the two halves of a store say about each other,
//!   read without opening anything that could change them. Nothing calls it
//!   to decide whether to open a store -- `SharedBlockStore::new` does that,
//!   before any pass runs -- but a tool that must REPORT the pairing needs it
//!   without paying the open's refusal.
//! - [`re_pair`]: the one repair, and the only recovery path the ADR grants.
//!   It rewrites the blocks root's marker to match the database's header, and
//!   it never does the reverse: the database is the half that knows which
//!   records it holds, so a store restored from backup takes the identity of
//!   the database that will serve it.
//!
//! Deliberately NOT a daemon flag. A `--force-pair` on the server would end
//! up in a unit file and defeat the check forever; a verb in the tool that
//! can audit the result keeps a human in the loop, once, at the moment the
//! decision is actually made.

use std::fmt::{self, Display, Formatter};
use std::io;
use std::path::{Path, PathBuf};

use crate::cas::BLOCKS_DB_DIR_NAME;
use crate::cas::block_disk::{StoreIdMarker, read_store_id_marker, rewrite_store_id_marker};
use crate::metastore::store_header::{self, StoreInit, classify_db_dir};
use crate::metastore::{FjallStore, MetaError, StoreId};

/// The shared blocks database under a metadata root, the way
/// `CasFS::single_namespace` builds it.
pub fn blocks_db_path(meta_root: &Path) -> PathBuf {
    meta_root.join("blocks").join(BLOCKS_DB_DIR_NAME)
}

/// The block file root under a data root.
pub fn blocks_root_path(fs_root: &Path) -> PathBuf {
    fs_root.join("blocks")
}

/// What each half of a store claims, and where the claim was read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pairing {
    /// The blocks database.
    pub db_path: PathBuf,
    /// The id its header carries, or `None` for a store that predates ADR
    /// 0012 and has not been opened by a build that adopts.
    pub header_id: Option<StoreId>,
    /// The blocks root.
    pub blocks_root: PathBuf,
    /// The id its marker carries, if it carries one that parses.
    pub marker_id: Option<StoreId>,
}

impl Pairing {
    /// Whether both halves name the same store. A half that claims nothing
    /// is not a match: it is an adoption waiting to happen, which is the
    /// open's job and not a verdict this can give.
    pub fn is_paired(&self) -> bool {
        matches!((self.header_id, self.marker_id), (Some(h), Some(m)) if h == m)
    }

    /// Whether the two halves name DIFFERENT stores: the refusal case.
    pub fn is_mispaired(&self) -> bool {
        matches!((self.header_id, self.marker_id), (Some(h), Some(m)) if h != m)
    }
}

/// Reads both halves of a store's pairing identity.
///
/// The database is opened (fjall's lock applies, as everywhere), the header
/// is read, and nothing is written -- not even the adoption the daemon's open
/// would perform. A tool that reports must not change what it reports on.
///
/// # Errors
///
/// [`PairingError::NoStore`] if there is no blocks database at `meta_root`
/// (this never creates one), [`PairingError::Open`] if it will not open --
/// including the routine "the daemon has it" lock contention -- and
/// [`PairingError::Marker`] if the blocks root cannot be read.
pub fn read_pairing(meta_root: &Path, fs_root: &Path) -> Result<Pairing, PairingError> {
    let db_path = blocks_db_path(meta_root);
    let blocks_root = blocks_root_path(fs_root);

    if classify_db_dir(&db_path).map_err(PairingError::Open)? == StoreInit::Create {
        return Err(PairingError::NoStore(db_path));
    }
    if !blocks_root.is_dir() {
        return Err(PairingError::NoBlocksRoot(blocks_root));
    }

    let header_id = {
        let store = FjallStore::new(db_path.clone(), Some(1), None).map_err(PairingError::Open)?;
        let header = store_header::read_header(&store, &db_path)
            .map_err(PairingError::Open)?
            .ok_or_else(|| PairingError::NoStore(db_path.clone()))?;
        header.store_id()
    };

    let marker_id = match read_store_id_marker(&blocks_root)
        .map_err(|e| PairingError::Marker(blocks_root.clone(), e))?
    {
        StoreIdMarker::Present(id) => Some(id),
        StoreIdMarker::Absent | StoreIdMarker::Unreadable(_) => None,
    };

    Ok(Pairing {
        db_path,
        header_id,
        blocks_root,
        marker_id,
    })
}

/// What a re-pair did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RePair {
    /// The identity both halves now agree on.
    pub store_id: StoreId,
    /// What the marker said before, if it said anything readable.
    pub was: Option<StoreId>,
    /// The database the id came from.
    pub db_path: PathBuf,
    /// The blocks root whose marker was (or was not) rewritten.
    pub blocks_root: PathBuf,
    /// False when the marker already matched: a re-pair is idempotent, and
    /// saying so is more useful than pretending to have done something.
    pub rewritten: bool,
}

impl RePair {
    /// The operator-facing account of what happened.
    pub fn render_text(&self) -> String {
        let mut out = String::new();
        if self.rewritten {
            out.push_str(&format!(
                "re-paired {} with the store at {}\n",
                self.blocks_root.display(),
                self.db_path.display()
            ));
        } else {
            out.push_str(&format!(
                "already paired: {} and {} both name one store\n",
                self.blocks_root.display(),
                self.db_path.display()
            ));
        }
        out.push_str(&format!("  store id:     {}\n", self.store_id));
        out.push_str(&format!(
            "  marker was:   {}\n",
            match self.was {
                Some(id) => id.to_hex(),
                None => "absent or unreadable".to_string(),
            }
        ));
        out.push_str(&format!("  marker now:   {}\n", self.store_id));
        out
    }
}

/// Rewrites the blocks root's marker to match the blocks database's header.
///
/// The authoritative recovery from a refused open, and the only one: after
/// this, that database and that blocks root are one store as far as every
/// build is concerned. Which is exactly why it takes both paths explicitly
/// and why the daemon cannot do it.
///
/// One direction only. A store whose header carries no id is refused rather
/// than minted into: adoption is the open's business, and a tool that minted
/// an identity here could hand a restored backup an id its own blocks root
/// never claimed.
///
/// # Errors
///
/// [`PairingError::NoStoreId`] if the database has no identity to copy (open
/// the store once; adoption mints one), plus everything
/// [`read_pairing`] can say.
pub fn re_pair(meta_root: &Path, fs_root: &Path) -> Result<RePair, PairingError> {
    let pairing = read_pairing(meta_root, fs_root)?;
    let Some(store_id) = pairing.header_id else {
        return Err(PairingError::NoStoreId(pairing.db_path));
    };

    let rewritten = pairing.marker_id != Some(store_id);
    if rewritten {
        rewrite_store_id_marker(&pairing.blocks_root, store_id)
            .map_err(|e| PairingError::Marker(pairing.blocks_root.clone(), e))?;
    }

    Ok(RePair {
        store_id,
        was: pairing.marker_id,
        db_path: pairing.db_path,
        blocks_root: pairing.blocks_root,
        rewritten,
    })
}

/// Why a pairing could not be read or repaired. Every variant is
/// could-not-run: none of them is a verdict about the store's contents.
#[derive(Debug)]
pub enum PairingError {
    /// No blocks database at that meta root. Never created here.
    NoStore(PathBuf),
    /// No blocks root at that fs root.
    NoBlocksRoot(PathBuf),
    /// The database would not open (locked by the daemon, refused header).
    Open(MetaError),
    /// The blocks root's marker could not be read or written.
    Marker(PathBuf, io::Error),
    /// The database carries no store id to copy.
    NoStoreId(PathBuf),
}

impl Display for PairingError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            PairingError::NoStore(path) => write!(f, "no store at {}", path.display()),
            PairingError::NoBlocksRoot(path) => {
                write!(f, "no blocks root at {}", path.display())
            }
            PairingError::Open(e) => write!(f, "{e}"),
            PairingError::Marker(path, e) => write!(
                f,
                "the store id marker in {} could not be read or written: {e}",
                path.display()
            ),
            PairingError::NoStoreId(path) => write!(
                f,
                "the store at {} has no store id yet, so there is nothing to pair to: open it \
                 once (the first open of a store that predates ADR 0012 mints one) and re-pair \
                 after that",
                path.display()
            ),
        }
    }
}

impl std::error::Error for PairingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PairingError::Open(e) => Some(e),
            PairingError::Marker(_, e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::crash_fixtures::{plant_unreadable_store_id_marker, remove_store_id_marker};
    use crate::cas::{SharedBlockStore, StorageEngine};
    use crate::metastore::Durability;
    use tempfile::{TempDir, tempdir};

    /// A store on two roots, closed again, and the id it minted.
    fn make_store(meta_root: &Path, fs_root: &Path) -> StoreId {
        SharedBlockStore::new(
            meta_root.join("blocks"),
            fs_root.join("blocks"),
            StorageEngine::Fjall,
            Some(1),
            Some(Durability::Buffer),
            None,
            None,
            None,
        )
        .expect("the store must open")
        .store_id()
        .expect("a created store has an id")
    }

    fn roots() -> (TempDir, TempDir, StoreId) {
        let meta = tempdir().unwrap();
        let fs = tempdir().unwrap();
        let id = make_store(meta.path(), fs.path());
        (meta, fs, id)
    }

    #[test]
    fn a_healthy_store_reads_as_paired_and_re_pairs_to_a_no_op() {
        let (meta, fs, id) = roots();

        let pairing = read_pairing(meta.path(), fs.path()).unwrap();
        assert!(pairing.is_paired());
        assert!(!pairing.is_mispaired());
        assert_eq!(pairing.header_id, Some(id));
        assert_eq!(pairing.marker_id, Some(id));

        let done = re_pair(meta.path(), fs.path()).unwrap();
        assert!(!done.rewritten, "nothing to do is not something to do");
        assert_eq!(done.store_id, id);
        assert!(done.render_text().contains("already paired"));
    }

    /// The recovery the ADR promises: a marker naming another store is
    /// rewritten to the database's id, and the store opens again afterwards.
    #[test]
    fn re_pair_makes_a_refused_pairing_open_again() {
        let (meta_a, fs_a, id_a) = roots();
        let (_meta_b, fs_b, id_b) = roots();

        // The mispairing: store A's database against store B's blocks root.
        let crossed = read_pairing(meta_a.path(), fs_b.path()).unwrap();
        assert!(crossed.is_mispaired());

        let done = re_pair(meta_a.path(), fs_b.path()).unwrap();
        assert!(done.rewritten);
        assert_eq!(done.store_id, id_a);
        assert_eq!(done.was, Some(id_b));
        let text = done.render_text();
        assert!(text.contains(&id_a.to_hex()), "{text}");
        assert!(text.contains(&id_b.to_hex()), "{text}");

        // The proof: the open that refused before now succeeds, and store
        // A's own root is untouched by any of it.
        assert_eq!(make_store(meta_a.path(), fs_b.path()), id_a);
        assert_eq!(
            read_pairing(meta_a.path(), fs_a.path()).unwrap().marker_id,
            Some(id_a)
        );
    }

    /// A marker that is missing or damaged is repaired the same way -- and
    /// the missing case is also what the next open would have done, so the
    /// verb is never the only way out of it.
    #[test]
    fn re_pair_writes_a_marker_that_is_absent_or_junk() {
        let (meta, fs, id) = roots();

        remove_store_id_marker(&fs.path().join("blocks"));
        let done = re_pair(meta.path(), fs.path()).unwrap();
        assert!(done.rewritten);
        assert_eq!(done.was, None);
        assert_eq!(done.store_id, id);

        plant_unreadable_store_id_marker(&fs.path().join("blocks"), b"\x00\x01 not an id");
        let done = re_pair(meta.path(), fs.path()).unwrap();
        assert!(done.rewritten);
        assert_eq!(done.was, None, "junk claims nothing");
        assert_eq!(done.store_id, id);
    }

    /// Nothing is created and nothing is guessed: a mistyped root is an
    /// error, not an empty store with a fresh identity.
    #[test]
    fn re_pair_refuses_paths_that_are_not_a_store() {
        let (meta, fs, _) = roots();
        let nowhere = tempdir().unwrap();

        let err = re_pair(nowhere.path(), fs.path()).unwrap_err();
        assert!(matches!(err, PairingError::NoStore(_)), "{err}");
        assert!(err.to_string().contains("no store at"), "{err}");
        assert!(
            !nowhere.path().join("blocks").exists(),
            "refusing means creating nothing"
        );

        let err = re_pair(meta.path(), nowhere.path()).unwrap_err();
        assert!(matches!(err, PairingError::NoBlocksRoot(_)), "{err}");
    }

    /// A store that predates ADR 0012 has no identity to copy. The tool says
    /// so and names the fix, rather than minting one of its own.
    #[test]
    fn re_pair_refuses_a_store_with_no_id_yet() {
        let (meta, fs, _) = roots();
        crate::cas::crash_fixtures::strip_store_id(&blocks_db_path(meta.path()));
        remove_store_id_marker(&fs.path().join("blocks"));

        let err = re_pair(meta.path(), fs.path()).unwrap_err();
        assert!(matches!(err, PairingError::NoStoreId(_)), "{err}");
        assert!(err.to_string().contains("open it once"), "{err}");

        // And the fix it names works: one open adopts, then re-pair is a
        // no-op because the open already wrote both halves.
        let id = make_store(meta.path(), fs.path());
        let done = re_pair(meta.path(), fs.path()).unwrap();
        assert!(!done.rewritten);
        assert_eq!(done.store_id, id);
    }
}
