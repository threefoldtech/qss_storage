//! Depth placement for new block files (ADR 0006).
//!
//! Under the full-id path scheme a file's NAME identifies its block, so the
//! fanout depth carries no correctness: any depth in `1..=width(id)` locates
//! the block. What remains is a performance decision -- keep directories
//! from growing unboundedly -- and an orphan-healing rule: if a file named
//! `<id>` already exists somewhere on the id's directory chain (crash
//! residue of an earlier attempt, complete or corrupt alike), reuse its
//! depth so the rewrite heals it in place instead of stranding it.
//!
//! Occupancy tracking is deliberately approximate: lazy in-process counters,
//! seeded by one `read_dir` count on first touch and bumped on every
//! placement. Approximation is harmless because placement has no
//! correctness content.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use faster_hex::hex_string;

use crate::metastore::BlockId;

/// A directory at or above this many entries stops receiving new block
/// files; placement moves one level deeper. With 256-way fanout per level
/// this bounds directories to a few thousand entries without ever needing
/// deep chains until the store is genuinely large.
pub(crate) const DEFAULT_MAX_DIR_ENTRIES: usize = 4096;

/// Chooses the fanout depth for new block files under one blocks root.
///
/// One instance per blocks root; namespaces sharing a block store share it.
#[derive(Debug)]
pub(crate) struct BlockPlacement {
    root: PathBuf,
    max_dir_entries: usize,
    /// Approximate entry counts of fanout directories, keyed by path
    /// relative form (the absolute dir path). Seeded lazily from `read_dir`,
    /// incremented on placement. Never trusted for correctness.
    counters: Mutex<HashMap<PathBuf, usize>>,
}

impl BlockPlacement {
    pub fn new(root: PathBuf) -> Self {
        Self::with_max_entries(root, DEFAULT_MAX_DIR_ENTRIES)
    }

    pub fn with_max_entries(root: PathBuf, max_dir_entries: usize) -> Self {
        Self {
            root,
            max_dir_entries: max_dir_entries.max(1),
            counters: Mutex::new(HashMap::new()),
        }
    }

    /// Picks the depth to write `id` at: the insert-time probe from ADR 0006.
    ///
    /// Walks the id's directory chain from the blocks root. At each level
    /// that exists on disk, a file named `<id>` found there means an orphan
    /// from an earlier attempt: its depth is returned so the rename heals it
    /// in place. The walk stops at the first missing child directory -- any
    /// candidate file must lie on the existing chain, so this bounds the
    /// probe to the chain's actual depth (worst case `width(id)` stats).
    ///
    /// With no orphan found, returns the placement-policy depth: the
    /// shallowest level whose target directory is (approximately) below the
    /// occupancy threshold.
    ///
    /// The chosen slot's counter is bumped, since the caller is about to
    /// place a file there.
    pub fn choose_depth(&self, id: &BlockId) -> u8 {
        if let Some(found) = self.probe_orphan(id) {
            return found;
        }
        let depth = self.policy_depth(id);
        self.record_placement(id, depth);
        depth
    }

    /// Walks the existing dir chain looking for a file named `<id>`.
    fn probe_orphan(&self, id: &BlockId) -> Option<u8> {
        let name = id.to_hex();
        let mut dir = self.root.clone();
        for (level, byte) in id.as_slice().iter().enumerate() {
            dir.push(hex_string(&[*byte]));
            if !dir.is_dir() {
                return None;
            }
            if dir.join(&name).is_file() {
                // level is 0-based; depth is the number of dir levels.
                #[allow(clippy::cast_possible_truncation)] // level < width <= 32
                return Some(level as u8 + 1);
            }
        }
        None
    }

    /// The shallowest depth whose target directory is below the threshold.
    fn policy_depth(&self, id: &BlockId) -> u8 {
        let width = id.len();
        let mut counters = self.counters.lock().expect("placement counters poisoned");
        let mut dir = self.root.clone();
        for (level, byte) in id.as_slice().iter().enumerate() {
            dir.push(hex_string(&[*byte]));
            let count = *counters
                .entry(dir.clone())
                .or_insert_with(|| approximate_entry_count(&dir));
            #[allow(clippy::cast_possible_truncation)] // level < width <= 32
            if count < self.max_dir_entries || level + 1 == width {
                return level as u8 + 1;
            }
        }
        // Unreachable: the loop always returns at level + 1 == width.
        #[allow(clippy::cast_possible_truncation)]
        {
            width as u8
        }
    }

    /// Bumps the counter of the directory a file is about to land in.
    fn record_placement(&self, id: &BlockId, depth: u8) {
        let mut dir = self.root.clone();
        for byte in &id.as_slice()[..(depth as usize).clamp(1, id.len())] {
            dir.push(hex_string(&[*byte]));
        }
        let mut counters = self.counters.lock().expect("placement counters poisoned");
        *counters
            .entry(dir.clone())
            .or_insert_with(|| approximate_entry_count(&dir)) += 1;
    }
}

/// One-shot seed for a directory's counter. A directory that does not exist
/// yet counts as empty.
fn approximate_entry_count(dir: &Path) -> usize {
    match std::fs::read_dir(dir) {
        Ok(entries) => entries.count(),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::{BLOCKID_SIZE, block_disk_path};
    use tempfile::tempdir;

    fn id_with_prefix(b0: u8, b1: u8) -> BlockId {
        let mut bytes = [0x55u8; BLOCKID_SIZE];
        bytes[0] = b0;
        bytes[1] = b1;
        BlockId::from(bytes)
    }

    #[test]
    fn fresh_store_places_at_depth_one() {
        let dir = tempdir().unwrap();
        let placement = BlockPlacement::new(dir.path().to_path_buf());
        assert_eq!(placement.choose_depth(&id_with_prefix(0xab, 0x01)), 1);
    }

    #[test]
    fn full_directory_pushes_one_level_deeper() {
        let dir = tempdir().unwrap();
        let placement = BlockPlacement::with_max_entries(dir.path().to_path_buf(), 2);

        // Two placements into ab/ fill it to the threshold; the third goes
        // to depth 2. Same first byte, different ids.
        assert_eq!(placement.choose_depth(&id_with_prefix(0xab, 0x01)), 1);
        assert_eq!(placement.choose_depth(&id_with_prefix(0xab, 0x02)), 1);
        assert_eq!(placement.choose_depth(&id_with_prefix(0xab, 0x03)), 2);
        // A different first byte still has room at depth 1.
        assert_eq!(placement.choose_depth(&id_with_prefix(0xcd, 0x01)), 1);
    }

    #[test]
    fn counter_seeds_from_existing_directory_contents() {
        let dir = tempdir().unwrap();
        let ab = dir.path().join("ab");
        std::fs::create_dir_all(&ab).unwrap();
        std::fs::write(ab.join("stale-1"), b"x").unwrap();
        std::fs::write(ab.join("stale-2"), b"x").unwrap();

        // Threshold 2, directory already holds 2 entries on disk: a fresh
        // placement instance (fresh counters) must see them and go deeper.
        let placement = BlockPlacement::with_max_entries(dir.path().to_path_buf(), 2);
        assert_eq!(placement.choose_depth(&id_with_prefix(0xab, 0x01)), 2);
    }

    #[test]
    fn orphan_on_the_chain_is_reused_for_heal_in_place() {
        let dir = tempdir().unwrap();
        let id = id_with_prefix(0xab, 0x01);

        // Plant an orphan at depth 2 while policy would say depth 1.
        let orphan = block_disk_path(&id, 2, dir.path().to_path_buf());
        std::fs::create_dir_all(orphan.parent().unwrap()).unwrap();
        std::fs::write(&orphan, b"partial garbage").unwrap();

        let placement = BlockPlacement::new(dir.path().to_path_buf());
        assert_eq!(placement.choose_depth(&id), 2, "must heal in place");

        // A different id on the same chain is unaffected by the orphan.
        assert_eq!(placement.choose_depth(&id_with_prefix(0xab, 0x02)), 1);
    }

    #[test]
    fn deep_orphan_on_a_complete_chain_is_found() {
        let dir = tempdir().unwrap();
        let id = id_with_prefix(0xab, 0x01);

        // Orphan at depth 3; creating it materializes its whole dir chain,
        // exactly as a crashed rename would have.
        let deep = block_disk_path(&id, 3, dir.path().to_path_buf());
        std::fs::create_dir_all(deep.parent().unwrap()).unwrap();
        std::fs::write(&deep, b"x").unwrap();

        let placement = BlockPlacement::new(dir.path().to_path_buf());
        assert_eq!(placement.choose_depth(&id), 3, "deep orphan must be found");

        // An id whose depth-1 chain dir does not exist stops the probe
        // immediately and takes the policy depth.
        let other = id_with_prefix(0xcd, 0x01);
        assert_eq!(
            placement.choose_depth(&other),
            1,
            "id with no chain dirs must take the policy depth"
        );
    }
}
