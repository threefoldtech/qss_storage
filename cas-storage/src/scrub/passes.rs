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

use chrono::Utc;

use crate::cas::multipart::MultiPart;
use crate::metastore::{
    BlockId, MULTIPART_PARTS_TREE, MetaError, UPLOADS_TREE, UploadRecord, block_disk_path,
};

use super::disk::DiskWalk;
use super::findings::{Finding, FindingClass};
use super::holders::{ExpectedCounts, HolderEnumerationError};
use super::records::RecordWalk;
use super::{ScrubContext, holders};

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

/// Pass 5: what the in-flight uploads are holding, and which part records
/// no upload owns.
///
/// Two trees, one walk each. `_UPLOADS` names every upload that exists
/// (ADR 0003: the record's existence IS the upload's), so it decides both
/// halves of this pass:
///
/// - one `multipart_upload` finding per upload record, carrying its AGE
///   alongside the part count and bytes its parts hold. An upload with no
///   parts yet is reported too -- it is still an upload an operator can see
///   and the TTL will still age it;
/// - one `orphan_part` finding per part record whose triple has no upload
///   record. Those hold their blocks with nothing left that can complete or
///   abort them: the residue of a crashed abort, of the accepted
///   upload_part-versus-abort race, of a part a completing client never
///   named, and of every legacy dash-keyed record. `--repair` reaps them
///   (the daemon GC is the primary reaper; this is the offline backstop).
///
/// Age is wall clock now minus the record's `created_at`, the same
/// subtraction the GC's TTL makes. fsck is an offline tool holding the
/// store's lock, so there is no monotonicity to preserve across a
/// concurrent writer, and a store carried to a machine with a wrong clock
/// reports a wrong age rather than doing anything about it (ADR 0003).
///
/// # Errors
///
/// [`HolderEnumerationError`] if a part record does not decode -- the same
/// refusal the holder walk makes, because a part record IS a holder -- or
/// if an UPLOAD record does not: without the full set of upload records a
/// live part cannot be told from an orphan, and the repair that follows
/// would release blocks an upload still owns. That is loss, so the pass
/// refuses rather than guessing, and the missing pass in `passes_run`
/// forbids the reaping downstream.
pub fn multipart_report(ctx: &ScrubContext) -> Result<Vec<Finding>, HolderEnumerationError> {
    /// Accumulated per (bucket, key, upload_id).
    #[derive(Default)]
    struct Upload {
        parts: u64,
        bytes: u64,
    }

    /// (bucket, key, upload_id): what a part record and an upload record
    /// both name, and the only thing that joins them.
    type Triple = (String, String, String);

    // Value-driven (ADR 0003 hard rule 4): both walks decode VALUES and
    // never parse a key, so the triple each record reports is its own.
    let mut records: HashMap<Triple, i64> = HashMap::new();
    for item in ctx.shared().uploads_tree().iter_all() {
        let (key, raw) = item.map_err(|source| HolderEnumerationError::Store {
            tree: Some(UPLOADS_TREE.to_string()),
            source,
        })?;
        let record = UploadRecord::try_from(&*raw).map_err(|e| {
            HolderEnumerationError::UndecodableRecord {
                tree: UPLOADS_TREE.to_string(),
                key: String::from_utf8_lossy(&key).into_owned(),
                source: MetaError::from(e),
            }
        })?;
        records.insert(
            (
                record.bucket().to_string(),
                record.key().to_string(),
                record.upload_id().to_string(),
            ),
            record.created_at(),
        );
    }

    let tree = ctx
        .shared()
        .meta_store()
        .get_tree_ext(MULTIPART_PARTS_TREE)
        .map_err(|source| HolderEnumerationError::Store {
            tree: Some(MULTIPART_PARTS_TREE.to_string()),
            source,
        })?;

    let mut uploads: HashMap<Triple, Upload> = HashMap::new();
    let mut orphans: Vec<Finding> = Vec::new();

    for item in tree.iter_all() {
        let (storage_key, raw) = item.map_err(|source| HolderEnumerationError::Store {
            tree: Some(MULTIPART_PARTS_TREE.to_string()),
            source,
        })?;
        let part =
            MultiPart::try_from(&*raw).map_err(|e| HolderEnumerationError::UndecodableRecord {
                tree: MULTIPART_PARTS_TREE.to_string(),
                key: String::from_utf8_lossy(&storage_key).into_owned(),
                source: MetaError::from(e),
            })?;

        let triple = (
            part.bucket().to_string(),
            part.key().to_string(),
            part.upload_id().to_string(),
        );
        if !records.contains_key(&triple) {
            // The key is carried whole, because that is the only address a
            // legacy dash-keyed record has: it cannot be rebuilt from the
            // triple, and reaping needs the key the walk yielded.
            orphans.push(
                Finding::new(
                    FindingClass::OrphanPart,
                    format!(
                        "part {} of upload {} ({}/{}): {} byte(s) held by a part record whose \
                         upload record does not exist, so nothing can complete or abort it. The \
                         daemon's GC reaps these on its next sweep; --repair reaps this one",
                        part.part_number(),
                        part.upload_id(),
                        part.bucket(),
                        part.key(),
                        part.size()
                    ),
                )
                .with_storage_key(&storage_key),
            );
            continue;
        }

        let entry = uploads.entry(triple).or_default();
        entry.parts += 1;
        entry.bytes += part.size() as u64;
    }

    // Wall clock, matching the wall clock `UploadRecord::new` stamped.
    let now = Utc::now().timestamp();
    let mut grouped: Vec<(Triple, i64)> = records.into_iter().collect();
    // Stable output: two runs over the same store report in the same order.
    // The orphans need no sort -- they come out in the tree's key order.
    grouped.sort_by(|(a, _), (b, _)| a.cmp(b));

    let mut findings: Vec<Finding> = grouped
        .into_iter()
        .map(|(triple, created_at)| {
            let held = uploads.remove(&triple).unwrap_or_default();
            let (bucket, key, upload_id) = triple;
            Finding::new(
                FindingClass::MultipartUpload,
                format!(
                    "upload {upload_id} of {bucket}/{key}: started {} ago, {} part(s), {} byte(s) \
                     held",
                    humanize_age(now - created_at),
                    held.parts,
                    held.bytes
                ),
            )
        })
        .collect();

    findings.extend(orphans);
    Ok(findings)
}

/// An age in seconds, for a person: days above a day, hours above an hour,
/// minutes above a minute, seconds below that.
///
/// Coarse on purpose. The number an operator acts on is "older than the
/// TTL", which is measured in days, and a second-exact age would suggest a
/// precision a wall-clock timestamp does not have. A record stamped in the
/// future -- a clock that went backwards, or a store carried between
/// machines -- is reported as a negative age rather than clamped to zero,
/// so the skew is visible instead of plausible.
fn humanize_age(seconds: i64) -> String {
    const MINUTE: i64 = 60;
    const HOUR: i64 = 60 * MINUTE;
    const DAY: i64 = 24 * HOUR;

    let (sign, magnitude) = if seconds < 0 {
        ("-", seconds.saturating_neg())
    } else {
        ("", seconds)
    };
    match magnitude {
        s if s >= DAY => format!("{sign}{} day(s)", s / DAY),
        s if s >= HOUR => format!("{sign}{} hour(s)", s / HOUR),
        s if s >= MINUTE => format!("{sign}{} minute(s)", s / MINUTE),
        s => format!("{sign}{s} second(s)"),
    }
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
mod tests {
    use super::*;
    use crate::scrub::disk::walk_disk;
    use crate::scrub::records::walk_records;
    use crate::scrub::tests::{plant_object, put, store, synthetic_id};
    use crate::scrub::{Severity, holders::expected_counts};

    use crate::cas::crash_fixtures::{
        plant_dangling_record, plant_degraded_record, plant_orphan_file, plant_upload_record,
    };
    use tempfile::tempdir;

    /// Finds the one finding of `class`, or fails loudly.
    fn only(findings: &[Finding], class: FindingClass) -> &Finding {
        let mut hits = findings.iter().filter(|f| f.class == class);
        let first = hits
            .next()
            .unwrap_or_else(|| panic!("no {class:?} in {findings:#?}"));
        assert!(hits.next().is_none(), "more than one {class:?}");
        first
    }

    #[tokio::test]
    async fn recount_reports_both_directions_and_the_missing_record() {
        let dir = tempdir().unwrap();
        let (shared, fs) = store(&dir);
        fs.create_bucket("b").unwrap();

        // Two objects, one block: rc 2, holders 2. Clean.
        let clean = put(&fs, "b", "one", b"clean block".repeat(20).to_vec()).await;
        put(&fs, "b", "two", b"clean block".repeat(20).to_vec()).await;

        // A record nothing references: the cancelled-PUT shape.
        let leaked = synthetic_id(0x91);
        plant_dangling_record(&shared, leaked, 1);

        // A holder referencing a block that has no record at all.
        let vanished = synthetic_id(0x92);
        plant_object(&fs, "b", "points-nowhere", vec![vanished]).await;

        // An under-count: three holders, a record that says one.
        let under = synthetic_id(0x93);
        plant_dangling_record(&shared, under, 1);
        plant_object(&fs, "b", "u1", vec![under, under, under]).await;

        let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
        let records = walk_records(&ctx).unwrap();
        let expected = expected_counts(&ctx).unwrap();
        let findings = recount(&expected, &records);

        assert!(
            !findings
                .iter()
                .any(|f| f.block.as_deref() == Some(clean.to_hex().as_str())),
            "an exact match is not a finding: {findings:#?}"
        );

        let over = only(&findings, FindingClass::RefcountOverCount);
        assert_eq!(over.block.as_deref(), Some(leaked.to_hex().as_str()));
        assert_eq!(over.severity, Severity::Info);

        let missing = only(&findings, FindingClass::MissingBlockRecord);
        assert_eq!(missing.block.as_deref(), Some(vanished.to_hex().as_str()));
        assert_eq!(missing.severity, Severity::Critical);

        let under_finding = only(&findings, FindingClass::RefcountUnderCount);
        assert_eq!(
            under_finding.block.as_deref(),
            Some(under.to_hex().as_str())
        );
        assert_eq!(under_finding.severity, Severity::Critical);
        assert!(under_finding.evidence.contains("rc 1"), "{under_finding:?}");
    }

    #[tokio::test]
    async fn disk_sweep_separates_orphans_off_depth_and_size_damage() {
        let dir = tempdir().unwrap();
        let (shared, fs) = store(&dir);
        fs.create_bucket("b").unwrap();

        let live = put(&fs, "b", "k", b"a live block".repeat(30).to_vec()).await;
        // A second copy of the live block at a depth the record does not name.
        plant_orphan_file(fs.fs_root(), &live, 3, b"stale copy");
        // A file no record mentions.
        let orphan = synthetic_id(0xa1);
        plant_orphan_file(fs.fs_root(), &orphan, 1, b"nobody's block");
        // Something that is not part of the layout.
        std::fs::write(fs.fs_root().join("NOTES.txt"), b"hello").unwrap();

        let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
        let records = walk_records(&ctx).unwrap();
        let findings = disk_sweep(&walk_disk(&ctx).unwrap(), &records);

        let off = only(&findings, FindingClass::OffDepthFile);
        assert_eq!(off.block.as_deref(), Some(live.to_hex().as_str()));
        assert_eq!(off.severity, Severity::Info);

        let orphan_finding = only(&findings, FindingClass::OrphanFile);
        assert_eq!(
            orphan_finding.block.as_deref(),
            Some(orphan.to_hex().as_str())
        );

        let foreign = only(&findings, FindingClass::ForeignFile);
        assert_eq!(foreign.severity, Severity::Warn);
        assert!(foreign.path.as_ref().unwrap().ends_with("NOTES.txt"));

        // No size damage yet: truncate the live file and the pass must see it.
        assert!(
            !findings
                .iter()
                .any(|f| f.class == FindingClass::SizeMismatch)
        );
        let live_path = records.records[&live].disk_path(&live, fs.fs_root().clone());
        std::fs::write(&live_path, b"short").unwrap();
        let findings = disk_sweep(&walk_disk(&ctx).unwrap(), &records);
        let size = only(&findings, FindingClass::SizeMismatch);
        assert_eq!(size.severity, Severity::Critical);
        assert!(size.evidence.contains("5 bytes"), "{size:?}");
    }

    /// The classification the repair depends on: a record whose id is on
    /// disk somewhere is adoptable, one whose id is nowhere is not.
    #[tokio::test]
    async fn adoptable_and_unrecoverable_dangling_records_are_different_classes() {
        let dir = tempdir().unwrap();
        let (shared, fs) = store(&dir);
        fs.create_bucket("b").unwrap();

        // Record says depth 1; the bytes are actually at depth 2.
        let adoptable = synthetic_id(0xb1);
        plant_dangling_record(&shared, adoptable, 1);
        plant_orphan_file(fs.fs_root(), &adoptable, 2, b"the missing bytes");

        // Record says depth 1 and nothing is anywhere.
        let gone = synthetic_id(0xb2);
        plant_dangling_record(&shared, gone, 1);
        plant_object(&fs, "b", "damaged", vec![gone]).await;

        let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
        let records = walk_records(&ctx).unwrap();
        let findings = dangling_sweep(&ctx, &records, &walk_disk(&ctx).unwrap());

        let candidate = only(&findings, FindingClass::AdoptableDanglingRecord);
        assert_eq!(
            candidate.block.as_deref(),
            Some(adoptable.to_hex().as_str())
        );
        assert_eq!(candidate.severity, Severity::Info);
        assert!(candidate.evidence.contains("depth 2"), "{candidate:?}");

        let dead = only(&findings, FindingClass::DanglingRecord);
        assert_eq!(dead.block.as_deref(), Some(gone.to_hex().as_str()));
        assert_eq!(dead.severity, Severity::Critical);
        assert!(dead.evidence.contains("poisons dedup"), "{dead:?}");
        assert_eq!(
            dead.holders,
            vec![crate::scrub::HolderRef::Object {
                bucket: "b".to_string(),
                key: "damaged".to_string(),
            }],
            "the blast radius must name the damaged object"
        );
    }

    /// One file, one finding. A misplaced file is an off-depth duplicate
    /// only while the record's own file is there to make it redundant; with
    /// the record's file gone, the same file is the adoption candidate and
    /// the disk sweep says nothing about it.
    #[test]
    fn a_misplaced_file_is_reported_once_by_whichever_pass_owns_it() {
        let dir = tempdir().unwrap();
        let (shared, fs) = store(&dir);
        let id = synthetic_id(0xe1);

        // Record at depth 1 with no file of its own; the bytes are at 2.
        plant_dangling_record(&shared, id, 1);
        plant_orphan_file(fs.fs_root(), &id, 2, b"the only copy");

        let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
        let records = walk_records(&ctx).unwrap();
        let disk = walk_disk(&ctx).unwrap();

        let sweep = disk_sweep(&disk, &records);
        assert!(
            sweep.is_empty(),
            "the candidate belongs to the dangling sweep: {sweep:#?}"
        );
        let dangling = dangling_sweep(&ctx, &records, &disk);
        assert_eq!(dangling.len(), 1, "{dangling:#?}");
        only(&dangling, FindingClass::AdoptableDanglingRecord);

        // Give the record its own file back: now the depth-2 copy really is
        // a redundant duplicate, and the disk sweep owns it.
        plant_orphan_file(fs.fs_root(), &id, 1, b"the only copy");
        let disk = walk_disk(&ctx).unwrap();

        let sweep = disk_sweep(&disk, &records);
        let off = only(&sweep, FindingClass::OffDepthFile);
        assert_eq!(off.severity, Severity::Info);
        assert!(off.path.as_ref().unwrap().contains("/e1/e1/"), "{off:?}");
        assert!(
            dangling_sweep(&ctx, &records, &disk).is_empty(),
            "the record's file is present, so nothing dangles"
        );
    }

    /// A degraded record is known damage: INFO, and never counted as a
    /// fresh dangling discovery.
    #[test]
    fn a_degraded_record_is_reported_as_known_damage() {
        let dir = tempdir().unwrap();
        let (shared, fs) = store(&dir);
        let degraded = synthetic_id(0xc1);
        plant_degraded_record(&shared, degraded, 1, 3, 4096);

        let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
        let records = walk_records(&ctx).unwrap();
        let findings = dangling_sweep(&ctx, &records, &walk_disk(&ctx).unwrap());

        assert_eq!(findings.len(), 1, "{findings:#?}");
        let finding = only(&findings, FindingClass::DegradedRecord);
        assert_eq!(finding.severity, Severity::Info);
        assert!(finding.evidence.contains("rc 3"), "{finding:?}");
    }

    #[tokio::test]
    async fn the_corruption_scrub_catches_a_flipped_bit() {
        let dir = tempdir().unwrap();
        let (shared, fs) = store(&dir);
        fs.create_bucket("b").unwrap();
        let id = put(&fs, "b", "k", b"bytes that will rot".repeat(10).to_vec()).await;

        let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
        let disk = walk_disk(&ctx).unwrap();
        assert!(
            corruption_scrub(&ctx, &disk).is_empty(),
            "an intact store scrubs clean"
        );

        // Flip a bit under the same name and length: only a re-hash sees it.
        let path = &disk.files[0].path;
        let mut bytes = std::fs::read(path).unwrap();
        bytes[0] ^= 0x01;
        std::fs::write(path, &bytes).unwrap();

        let findings = corruption_scrub(&ctx, &walk_disk(&ctx).unwrap());
        let corrupt = only(&findings, FindingClass::CorruptBlock);
        assert_eq!(corrupt.severity, Severity::Critical);
        assert_eq!(corrupt.block.as_deref(), Some(id.to_hex().as_str()));
        assert!(corrupt.evidence.contains("hash to"), "{corrupt:?}");
    }

    /// Seconds ago, as a `created_at` a planted upload record carries.
    fn started_ago(seconds: i64) -> i64 {
        Utc::now().timestamp() - seconds
    }

    /// Plants one part record of `upload_id`, naming one synthetic block.
    fn plant_part(
        fs: &crate::cas::CasFS,
        key: &str,
        upload_id: &str,
        part_number: i64,
        size: usize,
    ) {
        fs.insert_multipart_part(
            "b".to_string(),
            key.to_string(),
            size,
            part_number,
            upload_id.to_string(),
            crate::metastore::ContentHash::from([1u8; 16]),
            vec![synthetic_id(0xd0u8.wrapping_add(part_number as u8))],
        )
        .unwrap();
    }

    #[test]
    fn the_multipart_pass_groups_by_upload_and_reports_its_age() {
        let dir = tempdir().unwrap();
        let (shared, fs) = store(&dir);
        fs.create_bucket("b").unwrap();

        plant_upload_record(&fs, "b", "big", "u-1", started_ago(3 * 24 * 60 * 60));
        plant_part(&fs, "big", "u-1", 1, 1024);
        plant_part(&fs, "big", "u-1", 2, 512);
        // A second upload of the same key is a different unit of work, and
        // ages on its own clock.
        plant_upload_record(&fs, "b", "big", "u-2", started_ago(5 * 60 * 60));
        plant_part(&fs, "big", "u-2", 1, 64);
        // An upload nobody has sent a part for yet is still an upload.
        plant_upload_record(&fs, "b", "empty", "u-3", started_ago(90));

        let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
        let findings = multipart_report(&ctx).unwrap();

        assert_eq!(findings.len(), 3, "{findings:#?}");
        assert!(
            findings
                .iter()
                .all(|f| f.class == FindingClass::MultipartUpload && f.severity == Severity::Info)
        );

        let first = &findings[0];
        assert!(first.evidence.contains("u-1"), "{first:?}");
        assert!(first.evidence.contains("2 part(s)"), "{first:?}");
        assert!(first.evidence.contains("1536 byte(s)"), "{first:?}");
        assert!(
            first.evidence.contains("started 3 day(s) ago"),
            "the age ADR 0003 promised: {first:?}"
        );

        let second = &findings[1];
        assert!(second.evidence.contains("u-2"), "{second:?}");
        assert!(
            second.evidence.contains("started 5 hour(s) ago"),
            "an upload younger than a day reports hours: {second:?}"
        );

        // Sorted by (bucket, key, upload_id), so b/empty comes after b/big:
        // the upload nobody has uploaded a part for is reported all the same.
        let third = &findings[2];
        assert!(third.evidence.contains("u-3"), "{third:?}");
        assert!(
            third.evidence.contains("0 part(s), 0 byte(s)"),
            "an upload with no parts is reported on its age alone: {third:?}"
        );
        assert!(
            third.evidence.contains("started 1 minute(s) ago"),
            "{third:?}"
        );
    }

    /// The classification `--repair` acts on: the upload record decides. A
    /// part whose triple has one belongs to a live upload and is reported
    /// with it; a part whose triple has none is an orphan, reported on its
    /// own and carrying the raw key its reaping needs.
    #[test]
    fn parts_without_an_upload_record_are_orphans_not_uploads() {
        let dir = tempdir().unwrap();
        let (shared, fs) = store(&dir);
        fs.create_bucket("b").unwrap();

        plant_upload_record(&fs, "b", "big", "live", started_ago(60 * 60));
        plant_part(&fs, "big", "live", 1, 4096);
        // Same bucket and key, no upload record: the abort-race residue.
        plant_part(&fs, "big", "gone", 7, 2048);

        let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
        let findings = multipart_report(&ctx).unwrap();

        assert_eq!(findings.len(), 2, "{findings:#?}");
        let live = only(&findings, FindingClass::MultipartUpload);
        assert!(live.evidence.contains("upload live"), "{live:?}");
        assert!(live.evidence.contains("1 part(s)"), "{live:?}");

        let orphan = only(&findings, FindingClass::OrphanPart);
        assert_eq!(orphan.severity, Severity::Info);
        assert!(orphan.evidence.contains("part 7"), "{orphan:?}");
        assert!(orphan.evidence.contains("upload gone"), "{orphan:?}");
        assert!(orphan.evidence.contains("2048 byte(s)"), "{orphan:?}");
        assert!(
            orphan.evidence.contains("upload record does not exist"),
            "{orphan:?}"
        );
        // The raw storage key, so an operator can find the record and repair
        // can name what it reaped. An ADR 0003 key is length-prefixed, so it
        // is not printable text and renders as hex.
        let rendered = orphan.path.as_deref().expect("the key is carried");
        assert_eq!(
            rendered,
            faster_hex::hex_string(&crate::cas::multipart::part_key("b", "big", "gone", 7)),
            "{orphan:?}"
        );
    }

    /// A legacy dash-keyed record can have no upload record at all -- no
    /// point read can address it -- so it is an orphan by construction, and
    /// its key, being printable text, is carried as itself.
    #[test]
    fn a_legacy_dash_keyed_part_is_an_orphan_with_a_readable_key() {
        let dir = tempdir().unwrap();
        let (shared, fs) = store(&dir);
        fs.create_bucket("b").unwrap();

        let record = MultiPart::new(
            1024,
            1,
            "b".to_string(),
            "big".to_string(),
            "u-legacy".to_string(),
            crate::metastore::ContentHash::from([7u8; 16]),
            vec![synthetic_id(0xd9)],
        );
        shared
            .meta_store()
            .get_tree_ext(MULTIPART_PARTS_TREE)
            .unwrap()
            .insert(b"b-big-u-legacy-1", record.to_vec())
            .unwrap();

        let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
        let findings = multipart_report(&ctx).unwrap();

        let orphan = only(&findings, FindingClass::OrphanPart);
        assert_eq!(orphan.path.as_deref(), Some("b-big-u-legacy-1"));
    }

    /// An upload record that does not decode takes the pass down, exactly as
    /// an undecodable part record does: without the full set of upload
    /// records a live part cannot be told from an orphan, and reaping a live
    /// part is loss.
    #[test]
    fn an_undecodable_upload_record_refuses_the_pass() {
        let dir = tempdir().unwrap();
        let (shared, fs) = store(&dir);
        fs.create_bucket("b").unwrap();
        plant_part(&fs, "big", "u-1", 1, 128);

        shared
            .uploads_tree()
            .insert(b"not-a-real-key", vec![0xffu8; 3])
            .unwrap();

        let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
        match multipart_report(&ctx).unwrap_err() {
            HolderEnumerationError::UndecodableRecord { tree, .. } => {
                assert_eq!(tree, UPLOADS_TREE);
            }
            other => panic!("expected an undecodable-record refusal, got {other:?}"),
        }
    }

    /// Ages read as a person reads them, and a clock that went backwards is
    /// shown as the skew it is rather than as a plausible zero.
    #[test]
    fn ages_are_rendered_in_the_coarsest_unit_that_fits() {
        assert_eq!(humanize_age(0), "0 second(s)");
        assert_eq!(humanize_age(59), "59 second(s)");
        assert_eq!(humanize_age(60), "1 minute(s)");
        assert_eq!(humanize_age(59 * 60), "59 minute(s)");
        assert_eq!(humanize_age(60 * 60), "1 hour(s)");
        assert_eq!(humanize_age(47 * 60 * 60), "1 day(s)");
        assert_eq!(humanize_age(-90), "-1 minute(s)");
        // An absurd timestamp must not panic the report that carries it.
        assert!(humanize_age(i64::MIN).starts_with('-'));
    }

    #[tokio::test]
    async fn bucket_integrity_finds_a_tree_whose_row_is_gone() {
        let dir = tempdir().unwrap();
        let (shared, fs) = store(&dir);
        fs.create_bucket("kept").unwrap();
        fs.create_bucket("halfdeleted").unwrap();
        put(&fs, "halfdeleted", "k", b"still referenced".to_vec()).await;

        let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
        assert!(
            bucket_integrity(&ctx).unwrap().is_empty(),
            "both buckets have rows"
        );

        // Remove the row the way a crashed teardown would, leaving the tree.
        fs.namespace_meta_store()
            .get_allbuckets_tree()
            .unwrap()
            .remove(b"halfdeleted")
            .unwrap();

        let findings = bucket_integrity(&ctx).unwrap();
        let finding = only(&findings, FindingClass::HalfDeletedBucket);
        assert_eq!(finding.severity, Severity::Warn);
        assert!(finding.evidence.contains("halfdeleted"), "{finding:?}");
        assert!(
            finding.evidence.contains("1 object record"),
            "the stranded objects are sized: {finding:?}"
        );
    }
}
