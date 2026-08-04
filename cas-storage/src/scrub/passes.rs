//! The passes: what the three walkers' outputs mean.
//!
//! Each pass is a function over walker output, and the ones that can be
//! pure are pure -- the engine walks the store once and hands the same
//! results to every pass that needs them, so the record walk that powers
//! the recount is the same one that powers the dangling sweep.
//!
//! Severity is not decided here. Every class carries the ADR's severity via
//! [`FindingClass::default_severity`]; a pass that overrode it would move
//! the severity table out of its one home.

use std::collections::{HashMap, HashSet};

use crate::metastore::{BlockId, MetaError, block_disk_path};

use super::disk::DiskWalk;
use super::findings::{Finding, FindingClass};
use super::holders::ExpectedCounts;
use super::records::RecordWalk;
use super::{ScrubContext, holders};

mod multipart;

pub use multipart::multipart_report;

/// Pass 1: what the holders say versus what `_BLOCKS` says.
///
/// Over-counts are the expected direction and cost only space: cancellation
/// residue, and crashes between a record write and the release that should
/// have followed it. Under-counts are the dangerous direction -- a block
/// will be freed while something still references it -- and a holder
/// reference with no record at all means the data is already unreachable.
///
/// Since ADR 0008 a successful overwrite is no longer among the producers:
/// it releases what it displaced, so over-counts stopped being a routine
/// finding on a healthy store and went back to meaning something happened.
pub fn recount(expected: &ExpectedCounts, records: &RecordWalk) -> Vec<Finding> {
    let mut findings = Vec::new();

    for (id, holders) in expected {
        match records.records.get(id) {
            None => findings.push(
                Finding::new(
                    FindingClass::MissingBlockRecord,
                    format!("{holders} holder reference(s), but no block record"),
                )
                .with_block(id),
            ),
            Some(block) => {
                let rc = block.rc() as u64;
                if rc > *holders {
                    findings.push(
                        Finding::new(
                            FindingClass::RefcountOverCount,
                            format!(
                                "record rc {rc}, holders {holders}: {} reference(s) leaked",
                                rc - holders
                            ),
                        )
                        .with_block(id),
                    );
                } else if rc < *holders {
                    findings.push(
                        Finding::new(
                            FindingClass::RefcountUnderCount,
                            format!(
                                "record rc {rc}, holders {holders}: {} reference(s) unaccounted, \
                                 so the block can be freed while they still point at it",
                                holders - rc
                            ),
                        )
                        .with_block(id),
                    );
                }
            }
        }
    }

    // A record no holder mentions is the other half of the over-count: a
    // cancelled PUT, or an overwrite that crashed between committing the
    // new record and releasing the old one's blocks.
    for (id, block) in &records.records {
        if !expected.contains_key(id) {
            findings.push(
                Finding::new(
                    FindingClass::RefcountOverCount,
                    format!(
                        "record rc {}, holders 0: nothing references this block",
                        block.rc()
                    ),
                )
                .with_block(id),
            );
        }
    }

    findings
}

/// Pass 2: what is on disk that the records do not account for.
///
/// The size check applies only to the file at the record's own depth --
/// that is the one reads resolve to. A copy at any other depth is
/// unreferenced by construction, so its size means nothing.
///
/// One artifact, one finding: a copy at the wrong depth is only a
/// duplicate if the record's own file is actually there. When it is not,
/// this same file is the adoption candidate the dangling sweep reports, and
/// reporting it twice from opposite ends would make repair join findings to
/// work out that there is only one file.
pub fn disk_sweep(disk: &DiskWalk, records: &RecordWalk) -> Vec<Finding> {
    let mut findings = Vec::new();

    // What the walk actually saw, so this pass needs no stat of its own.
    let present: HashSet<(BlockId, u8)> = disk
        .files
        .iter()
        .map(|file| (file.id, file.depth))
        .collect();

    for file in &disk.files {
        match records.records.get(&file.id) {
            None => findings.push(
                Finding::new(
                    FindingClass::OrphanFile,
                    format!("{} bytes at depth {}, no record", file.size, file.depth),
                )
                .with_block(&file.id)
                .with_path(&file.path),
            ),
            Some(block) if block.depth() != file.depth => {
                if !present.contains(&(file.id, block.depth())) {
                    // The record has no file of its own: the dangling sweep
                    // owns this one, as the candidate to adopt.
                    continue;
                }
                findings.push(
                    Finding::new(
                        FindingClass::OffDepthFile,
                        format!(
                            "file at depth {}, but the record names depth {} and its file is \
                             there: unreferenced duplicate",
                            file.depth,
                            block.depth()
                        ),
                    )
                    .with_block(&file.id)
                    .with_path(&file.path),
                );
            }
            Some(block) => {
                let recorded = block.size() as u64;
                if recorded != file.size {
                    findings.push(
                        Finding::new(
                            FindingClass::SizeMismatch,
                            format!(
                                "file is {} bytes, record says {recorded}: the block is not what \
                                 the record describes",
                                file.size
                            ),
                        )
                        .with_block(&file.id)
                        .with_path(&file.path),
                    );
                }
            }
        }
    }

    findings.extend(
        disk.foreign
            .iter()
            .map(super::disk::ForeignPath::to_finding),
    );
    findings
}

/// Pass 3: records whose file is not where they say it is.
///
/// One stat per record, at the record's own depth -- the pure path builder
/// means no probing. What a miss means depends on what else is on disk:
///
/// - the same id at another depth: the bytes may still be there, so this is
///   an adoption candidate (repair re-hashes before trusting it);
/// - nothing: the bytes are gone. Every holder is damaged AND the record
///   poisons dedup, so this is the CRITICAL case, and the blast radius is
///   enumerated by a targeted holder re-walk.
///
/// A record already flagged degraded is reported as known damage awaiting a
/// heal, not as a fresh discovery.
pub fn dangling_sweep(ctx: &ScrubContext, records: &RecordWalk, disk: &DiskWalk) -> Vec<Finding> {
    // Where each id appears on disk, at whatever depth.
    let mut on_disk: HashMap<BlockId, Vec<u8>> = HashMap::new();
    for file in &disk.files {
        on_disk.entry(file.id).or_default().push(file.depth);
    }

    let root = ctx.blocks_root().to_path_buf();
    let mut findings = Vec::new();
    let mut unrecoverable: HashSet<BlockId> = HashSet::new();

    for (id, block) in &records.records {
        let path = block_disk_path(id, block.depth(), root.clone());
        let present = path.is_file();

        if block.is_degraded() {
            let state = if present {
                "a file is present at that path, so the next PUT of this content will heal it"
            } else {
                "no file, as expected of a degraded record"
            };
            findings.push(
                Finding::new(
                    FindingClass::DegradedRecord,
                    format!(
                        "record flagged degraded, rc {} (holders still accounted for); {state}",
                        block.rc()
                    ),
                )
                .with_block(id)
                .with_path(&path),
            );
            continue;
        }

        if present {
            continue;
        }

        let elsewhere: Vec<u8> = on_disk
            .get(id)
            .map(|depths| {
                depths
                    .iter()
                    .copied()
                    .filter(|d| *d != block.depth())
                    .collect()
            })
            .unwrap_or_default();

        if elsewhere.is_empty() {
            unrecoverable.insert(*id);
            findings.push(
                Finding::new(
                    FindingClass::DanglingRecord,
                    format!(
                        "no file at depth {}, and the id is nowhere else on disk: the bytes are \
                         gone. Every holder below is damaged, and until this record is repaired \
                         it poisons dedup -- a later PUT of the same content bumps this record \
                         instead of writing the file, committing another damaged object",
                        block.depth()
                    ),
                )
                .with_block(id)
                .with_path(&path),
            );
        } else {
            findings.push(
                Finding::new(
                    FindingClass::AdoptableDanglingRecord,
                    format!(
                        "no file at depth {}, but the id is on disk at depth {}: adoptable once \
                         the file is re-hashed",
                        block.depth(),
                        elsewhere
                            .iter()
                            .map(u8::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                )
                .with_block(id)
                .with_path(&path),
            );
        }
    }

    attach_blast_radius(ctx, &unrecoverable, &mut findings);
    findings
}

/// Fills in the holders of every unrecoverable block, in one targeted walk.
///
/// If the holder set cannot be closed the radius is simply unavailable --
/// the engine has already reported that refusal as its own CRITICAL
/// finding, and a dangling record is no less dangling for it.
fn attach_blast_radius(
    ctx: &ScrubContext,
    unrecoverable: &HashSet<BlockId>,
    findings: &mut [Finding],
) {
    if unrecoverable.is_empty() {
        return;
    }

    match holders::holders_of(ctx, unrecoverable) {
        Ok(radius) => {
            for finding in findings.iter_mut() {
                if finding.class != FindingClass::DanglingRecord {
                    continue;
                }
                let Some(hex) = finding.block.as_deref() else {
                    continue;
                };
                if let Some(holders) = radius
                    .iter()
                    .find(|(id, _)| id.to_hex() == hex)
                    .map(|(_, holders)| holders.clone())
                {
                    finding.holders = holders;
                }
            }
        }
        Err(e) => {
            for finding in findings.iter_mut() {
                if finding.class == FindingClass::DanglingRecord {
                    finding
                        .evidence
                        .push_str(&format!(" (blast radius unavailable: {e})"));
                }
            }
        }
    }
}

/// Pass 4: re-hash every block file against the address it is filed under.
///
/// The disk-bound pass, and the offline complement to verify-on-read: cold
/// data is never read, so nothing else ever checks it. Self-attributing --
/// the file's name is the expected hash, so no record lookup is needed.
///
/// A file that cannot be read is reported as corrupt too: the operator's
/// next move is the same, and swallowing it would let an unreadable block
/// pass for a verified one.
pub fn corruption_scrub(ctx: &ScrubContext, disk: &DiskWalk) -> Vec<Finding> {
    let hasher = ctx.shared().hasher();
    let mut findings = Vec::new();

    for file in &disk.files {
        match std::fs::read(&file.path) {
            Ok(bytes) => {
                let actual = hasher.hash(&bytes);
                if actual != file.id {
                    findings.push(
                        Finding::new(
                            FindingClass::CorruptBlock,
                            format!(
                                "{} bytes hash to {}, but the file is filed under {}",
                                bytes.len(),
                                actual.to_hex(),
                                file.id.to_hex()
                            ),
                        )
                        .with_block(&file.id)
                        .with_path(&file.path),
                    );
                }
            }
            Err(e) => findings.push(
                Finding::new(
                    FindingClass::CorruptBlock,
                    format!("could not be read, so it cannot be verified: {e}"),
                )
                .with_block(&file.id)
                .with_path(&file.path),
            ),
        }
    }

    findings
}

/// Pass 6: object trees whose bucket is gone.
///
/// `bucket_delete` removes the `_BUCKETS` row before tearing the objects
/// down, so a crash mid-loop strands an invisible tree whose records still
/// hold block references. The recount stays truthful either way -- the
/// holder walk enumerates trees, not bucket rows -- so this is an
/// inconsistency to resume, not a loss.
///
/// # Errors
///
/// [`MetaError`] if the tree list or the bucket list cannot be read.
pub fn bucket_integrity(ctx: &ScrubContext) -> Result<Vec<Finding>, MetaError> {
    let namespace = ctx.namespace();
    // By NAME, from the keys: what a bucket row's value holds is the
    // service's business (respcas keeps its namespace metadata there), and
    // this pass only asks which rows exist.
    let named: HashSet<String> = namespace.list_bucket_names()?.into_iter().collect();

    let mut orphaned: Vec<String> = namespace
        .list_trees()?
        .into_iter()
        .filter(|tree| !tree.starts_with('_') && !named.contains(tree))
        .collect();
    orphaned.sort();

    let mut findings = Vec::new();
    for tree in orphaned {
        // One count per stranded tree, to size the leak for the operator.
        let objects = namespace
            .get_tree_ext(&tree)
            .and_then(|t| t.len())
            .map_or_else(|e| format!("an unknown number of ({e})"), |n| n.to_string());
        findings.push(Finding::new(
            FindingClass::HalfDeletedBucket,
            format!(
                "object tree {tree} has no _BUCKETS row: a bucket teardown that did not finish. \
                 It still holds {objects} object record(s), and their block references are still \
                 counted"
            ),
        ));
    }

    Ok(findings)
}

#[cfg(test)]
mod tests;
