//! The holder walk: who references which block, counted per occurrence.
//!
//! This is the one place reference classes are enumerated, and the only
//! place in the scrub that refuses rather than reports. The asymmetry is
//! the point (ADR 0005 hard rule 1): a recount over a partial holder set
//! that then "repairs" frees live blocks -- loss by repair, the one failure
//! mode this tool must structurally exclude. So every failure here, from a
//! tree that will not open to a single record that will not decode, aborts
//! the enumeration and takes the recount and all repair down with it.
//!
//! Two reference classes:
//!
//! - object records in every non-reserved tree of the namespace DB. The
//!   trees come from [`MetaStore::list_trees`], NOT from the `_BUCKETS`
//!   rows: a bucket whose teardown crashed after its row was removed still
//!   has an object tree, and that tree still holds references.
//! - part records in `_MULTIPART_PARTS`, unconditionally. With ADR 0003
//!   unimplemented, part records are the only holders of an in-flight
//!   upload's blocks; skipping them would recount a live upload to zero.
//!
//! Iteration is `iter_all` throughout, never `range_filter` -- that one
//! drops records it cannot decode, which is precisely the event this walk
//! must refuse on.

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display, Formatter};

use crate::cas::multipart::MultiPart;
use crate::metastore::{BlockId, MULTIPART_PARTS_TREE, MetaError, Object};

use super::ScrubContext;
use super::findings::HolderRef;

/// How many times each block is referenced by a holder, counted per
/// occurrence: a block listed twice in one object counts twice, which is
/// what the write path's every-hit-bumps rule produces.
pub type ExpectedCounts = HashMap<BlockId, u64>;

/// Why the holder enumeration refused to produce a count.
///
/// Both variants mean the same thing to the caller -- the holder set is not
/// closed, so no recount may run -- and differ only in what to tell the
/// operator.
#[derive(Debug)]
pub enum HolderEnumerationError {
    /// The store would not answer: the tree listing, a tree open, or a step
    /// of an iteration failed.
    Store {
        /// The tree being walked, or `None` when the tree listing itself
        /// failed.
        tree: Option<String>,
        /// What the store said.
        source: MetaError,
    },
    /// A holder record would not decode. Its references cannot be counted,
    /// so nothing derived from this walk is trustworthy.
    UndecodableRecord {
        /// Tree the record lives in.
        tree: String,
        /// Record key, rendered lossily -- a key is bytes, not necessarily
        /// UTF-8.
        key: String,
        /// What the decoder said.
        source: MetaError,
    },
}

impl Display for HolderEnumerationError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            HolderEnumerationError::Store {
                tree: Some(t),
                source,
            } => write!(
                f,
                "holder enumeration incomplete: tree {t} could not be walked: {source}"
            ),
            HolderEnumerationError::Store { tree: None, source } => write!(
                f,
                "holder enumeration incomplete: the store would not list its trees: {source}"
            ),
            HolderEnumerationError::UndecodableRecord { tree, key, source } => write!(
                f,
                "holder enumeration incomplete: record {key} in tree {tree} does not decode: {source}"
            ),
        }
    }
}

impl std::error::Error for HolderEnumerationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            HolderEnumerationError::Store { source, .. }
            | HolderEnumerationError::UndecodableRecord { source, .. } => Some(source),
        }
    }
}

/// One decoded holder record, borrowed for the duration of a visit.
///
/// The visitor gets the blocks without paying for a [`HolderRef`]: the main
/// walk only counts, and allocating an owned bucket and key per object
/// record would double the cost of the cheapest pass for nothing.
enum HolderRecord<'a> {
    Object {
        tree: &'a str,
        key: &'a [u8],
        object: &'a Object,
    },
    Part {
        part: &'a MultiPart,
    },
}

impl HolderRecord<'_> {
    /// The blocks this holder references, in order, with repeats.
    fn blocks(&self) -> &[BlockId] {
        match self {
            HolderRecord::Object { object, .. } => object.blocks(),
            HolderRecord::Part { part } => part.blocks(),
        }
    }

    /// The reportable identity of this holder. Only built when a caller
    /// actually needs it.
    fn holder_ref(&self) -> HolderRef {
        match self {
            HolderRecord::Object { tree, key, .. } => HolderRef::Object {
                bucket: (*tree).to_string(),
                key: String::from_utf8_lossy(key).into_owned(),
            },
            HolderRecord::Part { part } => HolderRef::Part {
                bucket: part.bucket().to_string(),
                key: part.key().to_string(),
                upload_id: part.upload_id().to_string(),
                part_number: part.part_number(),
            },
        }
    }
}

/// Whether a tree name is one of the store's own rather than a bucket.
///
/// The store reserves the whole `_` namespace and refuses to create a bucket
/// in it (`MetaStore::insert_bucket`), so this test is exact rather than a
/// list of known names that a new internal tree could fall off.
fn is_reserved(tree: &str) -> bool {
    tree.starts_with('_')
}

/// Visits every holder record in the store exactly once.
///
/// Returns on the first failure of any kind: this is the closed-holder-set
/// refusal, not a best-effort walk.
fn walk_holders<F>(ctx: &ScrubContext, mut visit: F) -> Result<(), HolderEnumerationError>
where
    F: FnMut(&HolderRecord<'_>),
{
    let namespace = ctx.namespace();

    let trees = namespace
        .list_trees()
        .map_err(|source| HolderEnumerationError::Store { tree: None, source })?;

    for tree_name in trees.iter().filter(|name| !is_reserved(name)) {
        let tree =
            namespace
                .get_tree_ext(tree_name)
                .map_err(|source| HolderEnumerationError::Store {
                    tree: Some(tree_name.clone()),
                    source,
                })?;

        for item in tree.iter_all() {
            let (key, raw) = item.map_err(|source| HolderEnumerationError::Store {
                tree: Some(tree_name.clone()),
                source,
            })?;
            let object =
                Object::try_from(&*raw).map_err(|e| HolderEnumerationError::UndecodableRecord {
                    tree: tree_name.clone(),
                    key: String::from_utf8_lossy(&key).into_owned(),
                    source: MetaError::from(e),
                })?;
            visit(&HolderRecord::Object {
                tree: tree_name,
                key: &key,
                object: &object,
            });
        }
    }

    // The part records live in the SHARED block DB, not the namespace DB.
    let parts = ctx
        .shared()
        .meta_store()
        .get_tree_ext(MULTIPART_PARTS_TREE)
        .map_err(|source| HolderEnumerationError::Store {
            tree: Some(MULTIPART_PARTS_TREE.to_string()),
            source,
        })?;

    for item in parts.iter_all() {
        let (key, raw) = item.map_err(|source| HolderEnumerationError::Store {
            tree: Some(MULTIPART_PARTS_TREE.to_string()),
            source,
        })?;
        let part =
            MultiPart::try_from(&*raw).map_err(|e| HolderEnumerationError::UndecodableRecord {
                tree: MULTIPART_PARTS_TREE.to_string(),
                key: String::from_utf8_lossy(&key).into_owned(),
                source: MetaError::from(e),
            })?;
        visit(&HolderRecord::Part { part: &part });
    }

    Ok(())
}

/// Counts every block reference every holder holds.
///
/// # Errors
///
/// [`HolderEnumerationError`] on the first tree that will not open, step
/// that will not read, or record that will not decode. There is no partial
/// result on purpose.
pub fn expected_counts(ctx: &ScrubContext) -> Result<ExpectedCounts, HolderEnumerationError> {
    let mut counts: ExpectedCounts = HashMap::new();
    walk_holders(ctx, |holder| {
        for block in holder.blocks() {
            *counts.entry(*block).or_insert(0) += 1;
        }
    })?;
    Ok(counts)
}

/// The blast radius of `blocks`: which holders reference each of them.
///
/// A second, targeted walk. The main walk deliberately keeps no reverse
/// index -- one entry per reference on a large store is the memory the
/// recount is trying not to spend -- so this pays a re-read for the handful
/// of blocks a report actually names.
///
/// Blocks with no holder are absent from the result rather than present and
/// empty.
///
/// # Errors
///
/// As [`expected_counts`]: any incompleteness is a refusal.
pub fn holders_of(
    ctx: &ScrubContext,
    blocks: &HashSet<BlockId>,
) -> Result<HashMap<BlockId, Vec<HolderRef>>, HolderEnumerationError> {
    let mut found: HashMap<BlockId, Vec<HolderRef>> = HashMap::new();
    if blocks.is_empty() {
        return Ok(found);
    }

    walk_holders(ctx, |holder| {
        // One holder may reference the same block twice; it is still one
        // holder, so the id set is deduplicated per record.
        let mut hit: HashSet<BlockId> = HashSet::new();
        for block in holder.blocks() {
            if blocks.contains(block) && hit.insert(*block) {
                found.entry(*block).or_default().push(holder.holder_ref());
            }
        }
    })?;

    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::ObjectData;

    /// Pins the reference classes at the scrub's own boundary.
    ///
    /// `Object::blocks()` matches exhaustively already, but decode dispatches
    /// on a type byte, so a new `ObjectData` variant would compile there
    /// against a wildcard nobody notices. This match has no wildcard arm: a
    /// new variant fails the build HERE, in the module whose correctness
    /// depends on knowing every way an object can hold a reference.
    #[test]
    fn every_object_data_variant_is_accounted_for() {
        fn holds_references(data: &ObjectData) -> bool {
            match data {
                // Inline objects hold their bytes, not block references.
                ObjectData::Inline { .. } => false,
                ObjectData::SinglePart { blocks } => !blocks.is_empty(),
                ObjectData::MultiPart { blocks, .. } => !blocks.is_empty(),
            }
        }

        assert!(!holds_references(&ObjectData::Inline {
            data: vec![1, 2, 3]
        }));
        assert!(!holds_references(&ObjectData::SinglePart {
            blocks: vec![]
        }));
        assert!(holds_references(&ObjectData::SinglePart {
            blocks: vec![BlockId::from([1u8; crate::metastore::BLOCKID_SIZE])],
        }));
        assert!(holds_references(&ObjectData::MultiPart {
            blocks: vec![BlockId::from([2u8; crate::metastore::BLOCKID_SIZE])],
            parts: 1,
        }));
    }

    #[test]
    fn reserved_trees_are_recognised_by_prefix() {
        assert!(is_reserved("_BLOCKS"));
        assert!(is_reserved("_MULTIPART_PARTS"));
        assert!(is_reserved("_STORE_HEADER"));
        assert!(is_reserved("_"));
        assert!(!is_reserved("photos"));
        assert!(!is_reserved("my_bucket"));
    }
}
