//! The disk walk: everything under the blocks root, classified by structure
//! alone.
//!
//! The ADR 0006 layout makes this possible without a single DB lookup: a
//! block file's NAME is the full hex of its own address, and the directories
//! above it are the hex bytes of that same address's prefix. So the walker
//! can decide what every entry is by looking at it, and every file it
//! accepts round-trips: for a yielded `(id, depth)`,
//! `block_disk_path(id, depth, root)` is the path it was found at.
//!
//! That round-trip is the acceptance rule, and it is stricter than "the name
//! parses". A file whose name is valid hex but which sits under a directory
//! chain that is not its own address prefix is unreachable -- readers only
//! ever look where the derived path says -- so it is foreign, not a
//! misplaced block. Likewise a file directly under the root: `depth` is
//! clamped to at least 1, so no record can ever name it.

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};

use crate::cas::block_disk::{QUARANTINE_DIR_NAME, TMP_DIR_NAME};
use crate::metastore::BlockId;

use super::ScrubContext;
use super::findings::{Finding, FindingClass};

/// A block file the layout accepts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockFile {
    /// Address the file names itself with.
    pub id: BlockId,
    /// Fanout depth it sits at: the number of directory levels below the
    /// blocks root.
    pub depth: u8,
    /// Size in bytes.
    pub size: u64,
    /// Where it is. Kept typed (unlike the finding's lossy rendering)
    /// because repair acts on it.
    pub path: PathBuf,
}

/// Something under the blocks root that is not part of this store's layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignPath {
    /// Where it is.
    pub path: PathBuf,
    /// Why it was rejected, for the operator.
    pub reason: String,
}

impl ForeignPath {
    /// Renders this as the WARN finding the report carries.
    pub fn to_finding(&self) -> Finding {
        Finding::new(FindingClass::ForeignFile, self.reason.clone()).with_path(&self.path)
    }
}

/// What the disk walk found.
#[derive(Debug, Default)]
pub struct DiskWalk {
    /// Every accepted block file.
    pub files: Vec<BlockFile>,
    /// Everything else.
    pub foreign: Vec<ForeignPath>,
}

/// Walks the blocks root recursively.
///
/// `.tmp` and `.quarantine` are skipped, at the top level only: `.tmp` is
/// purged wholesale at store open and is never fsck's business, and
/// `.quarantine` holds what a previous repair already set aside. Deeper
/// down, a directory by either name is not a fanout directory and is
/// reported foreign like anything else.
///
/// The store's own metadata files are skipped too, wherever they turn up
/// ([`ScrubContext::store_own_paths`]): the default layout runs the tools
/// with one root for both (`--meta-root . --fs-root .`), which puts the
/// shared block database and its header sidecar *inside* the blocks root.
/// They are not block data and they are not foreign -- they are the store.
///
/// # Errors
///
/// [`io::Error`] if a directory cannot be read. Deliberately fatal: a
/// subtree that could not be listed would make every record whose file
/// lives there look dangling.
pub fn walk_disk(ctx: &ScrubContext) -> io::Result<DiskWalk> {
    let mut walk = DiskWalk::default();
    let root = ctx.blocks_root().to_path_buf();
    if !root.exists() {
        return Ok(walk);
    }
    walk_dir(
        &root,
        &[],
        ctx.id_width(),
        &ctx.store_own_paths(),
        &mut walk,
        true,
    )?;
    Ok(walk)
}

/// One directory level. `chain` is the byte sequence spelled by the
/// directories walked so far, which an accepted file's address must start
/// with.
fn walk_dir(
    dir: &Path,
    chain: &[u8],
    id_width: usize,
    store_own: &HashSet<PathBuf>,
    walk: &mut DiskWalk,
    at_root: bool,
) -> io::Result<()> {
    let mut entries: Vec<_> = std::fs::read_dir(dir)?.collect::<Result<Vec<_>, _>>()?;
    // Deterministic order: two runs over the same tree must produce the same
    // report.
    entries.sort_by_key(std::fs::DirEntry::file_name);

    for entry in entries {
        let path = entry.path();
        if store_own.contains(&path) {
            continue;
        }
        let file_type = entry.file_type()?;

        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            walk.foreign.push(ForeignPath {
                path,
                reason: "name is not valid UTF-8, so it cannot be a block address".to_string(),
            });
            continue;
        };

        if at_root && (name == TMP_DIR_NAME || name == QUARANTINE_DIR_NAME) {
            continue;
        }

        if file_type.is_dir() {
            match parse_hex_byte(&name) {
                Some(byte) => {
                    let mut deeper = chain.to_vec();
                    deeper.push(byte);
                    walk_dir(&path, &deeper, id_width, store_own, walk, false)?;
                }
                None => walk.foreign.push(ForeignPath {
                    path,
                    reason: format!(
                        "directory {name} is not a fanout level (expected two lowercase hex digits)"
                    ),
                }),
            }
            continue;
        }

        if !file_type.is_file() {
            walk.foreign.push(ForeignPath {
                path,
                reason: "not a regular file or directory".to_string(),
            });
            continue;
        }

        match classify_file(&name, chain, id_width) {
            Ok(id) => {
                let size = entry.metadata()?.len();
                walk.files.push(BlockFile {
                    id,
                    // chain.len() is bounded by the id width (32), because a
                    // longer chain cannot be a prefix of any address.
                    #[allow(clippy::cast_possible_truncation)]
                    depth: chain.len() as u8,
                    size,
                    path,
                });
            }
            Err(reason) => walk.foreign.push(ForeignPath { path, reason }),
        }
    }

    Ok(())
}

/// Decides whether `name`, sitting under the directory chain `chain`, is a
/// block file of this store.
fn classify_file(name: &str, chain: &[u8], id_width: usize) -> Result<BlockId, String> {
    let Some(bytes) = parse_lower_hex(name, id_width) else {
        return Err(format!(
            "file name {name} is not a {id_width} byte address in lowercase hex"
        ));
    };

    if chain.is_empty() {
        return Err(format!(
            "file {name} sits directly under the blocks root, where no record can name it \
             (the shallowest derived path is one fanout level deep)"
        ));
    }

    if !bytes.starts_with(chain) {
        return Err(format!(
            "file {name} sits under {}, which is not its own address prefix, so no derived \
             path resolves to it",
            faster_hex::hex_string(chain)
        ));
    }

    BlockId::from_slice(&bytes).map_err(|e| format!("file name {name} is not an address: {e}"))
}

/// Parses exactly two lowercase hex digits into the byte they spell.
fn parse_hex_byte(name: &str) -> Option<u8> {
    parse_lower_hex(name, 1).map(|bytes| bytes[0])
}

/// Parses `len` bytes' worth of lowercase hex, or nothing.
///
/// Lowercase only, and the length must be exact: the writer produces
/// lowercase full-width names, so anything else was not written by this
/// store and must not be mistaken for one of its blocks.
fn parse_lower_hex(name: &str, len: usize) -> Option<Vec<u8>> {
    if name.len() != len * 2 {
        return None;
    }
    let mut out = Vec::with_capacity(len);
    for pair in name.as_bytes().chunks_exact(2) {
        let hi = lower_hex_digit(pair[0])?;
        let lo = lower_hex_digit(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

/// One lowercase hex digit's value.
fn lower_hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_parsing_is_exact_and_lowercase() {
        assert_eq!(parse_hex_byte("ab"), Some(0xab));
        assert_eq!(parse_hex_byte("00"), Some(0x00));
        assert_eq!(parse_hex_byte("ff"), Some(0xff));
        // Uppercase is not what the writer produces.
        assert_eq!(parse_hex_byte("AB"), None);
        assert_eq!(parse_hex_byte("a"), None);
        assert_eq!(parse_hex_byte("abc"), None);
        assert_eq!(parse_hex_byte("gg"), None);
        assert_eq!(parse_hex_byte(".tmp"), None);
    }

    #[test]
    fn file_names_must_be_full_width_and_on_their_own_chain() {
        let name = "ab01".to_string() + &"00".repeat(14);
        assert!(classify_file(&name, &[0xab], 16).is_ok());
        // Right name, wrong chain.
        assert!(classify_file(&name, &[0xcd], 16).is_err());
        // Right name, no chain at all.
        assert!(classify_file(&name, &[], 16).is_err());
        // Right chain, half a name.
        assert!(classify_file("ab01", &[0xab], 16).is_err());
        // Right name, wrong store width.
        assert!(classify_file(&name, &[0xab], 32).is_err());
    }
}
