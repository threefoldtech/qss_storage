//! The engine: what to plan from a report, in application order, and what
//! it means for a repairable finding to survive the run.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::cas::CasFS;
use crate::cas::multipart::MultiPart;
use crate::metastore::{BlockId, MULTIPART_PARTS_TREE, MetaError, MetaStore, block_disk_path};
use crate::scrub::disk::{DiskWalk, walk_disk};
use crate::scrub::engine::{self, ScrubOptions};
use crate::scrub::findings::{Finding, FindingClass, Severity};
use crate::scrub::holders::expected_counts;
use crate::scrub::records::walk_records;
use crate::scrub::report::{Pass, Report};

use super::summary::{RepairOutcome, RepairStatus, RepairSummary};
use super::{RepairAction, RepairContext, RepairError};

// ---------------------------------------------------------------------
// The engine
// ---------------------------------------------------------------------

/// Applies what the report found, then checks its work.
///
/// Two planning rounds, because one action changes what the others must do.
/// Round one is the actions that remove HOLDERS -- resuming a half-deleted
/// bucket's teardown removes object records, reaping an orphan part removes
/// a part record -- so every rc below them is counted after they are done,
/// never before. A recount planned beside them would set each rc to a count
/// that still included the holder about to go.
///
/// `options` decides the post-repair passes; pass the same ones the report
/// was produced with, or the check will not look at what the repair
/// touched.
///
/// # Errors
///
/// [`RepairError`] if the store could not be walked or the post-repair
/// passes could not run. A failed individual action is not an error: it is
/// an outcome, and the post-repair report carries its consequence.
pub async fn repair(
    ctx: &RepairContext<'_>,
    report: &Report,
    options: &ScrubOptions,
) -> Result<RepairSummary, RepairError> {
    let mut outcomes: Vec<RepairOutcome> = Vec::new();

    for action in plan_teardowns(ctx, report, &mut outcomes)? {
        outcomes.extend(action.apply(ctx).await);
    }

    for action in plan_orphan_parts(ctx, report, &mut outcomes)? {
        outcomes.extend(action.apply(ctx).await);
    }

    for action in plan_repairs(ctx, report, &mut outcomes)? {
        outcomes.extend(action.apply(ctx).await);
    }

    // Hard rule 2: the passes run again over the repaired store, and
    // anything they still find that this engine claims to repair means an
    // action did not take.
    let post = engine::run(&ctx.scrub_context(), options).map_err(RepairError::Scrub)?;
    Ok(RepairSummary::new(outcomes, flag_unfinished(post)))
}

/// Round one: the half-deleted buckets.
///
/// Gated on the recount for the same reason [`RepairAction::SetRc`] is --
/// the teardown drives refcounts down through the delete path, and no
/// refcount may move while the holder set is unclosed.
fn plan_teardowns(
    ctx: &RepairContext<'_>,
    report: &Report,
    outcomes: &mut Vec<RepairOutcome>,
) -> Result<Vec<RepairAction>, RepairError> {
    if !report.ran(Pass::BucketIntegrity) {
        return Ok(Vec::new());
    }

    let stranded = stranded_trees(ctx.namespace()).map_err(RepairError::Store)?;
    if stranded.is_empty() {
        return Ok(Vec::new());
    }

    if !report.ran(Pass::Recount) {
        outcomes.push(
            RepairOutcome::new(
                "resume_bucket_teardown",
                RepairStatus::Skipped,
                format!(
                    "{} half-deleted bucket(s) left alone: the report's holder set was not \
                     closed, and a teardown decrements refcounts",
                    stranded.len()
                ),
            )
            .at(Severity::Warn),
        );
        return Ok(Vec::new());
    }

    Ok(stranded
        .into_iter()
        .map(|bucket| RepairAction::ResumeBucketTeardown { bucket })
        .collect())
}

/// Round one, second half: the part records no upload record owns (ADR
/// 0003).
///
/// Gated twice. On [`Pass::MultipartReport`], because that pass is what
/// closes the classification: an upload record that will not decode takes
/// it down, and without the full set of upload records a live upload's part
/// cannot be told from an orphan -- reaping one would release references an
/// upload still owns, which is loss. And on [`Pass::Recount`], like every
/// action whose consequence is a refcount: the reap releases the references
/// the part record held.
///
/// The daemon's GC is the primary reaper of these; this is the offline
/// backstop for a store whose daemon has not run (ADR 0003 decision 9).
fn plan_orphan_parts(
    ctx: &RepairContext<'_>,
    report: &Report,
    outcomes: &mut Vec<RepairOutcome>,
) -> Result<Vec<RepairAction>, RepairError> {
    if !report.ran(Pass::MultipartReport) {
        return Ok(Vec::new());
    }

    let orphans = orphan_parts(ctx.fs).map_err(RepairError::Store)?;
    if orphans.is_empty() {
        return Ok(Vec::new());
    }

    if !report.ran(Pass::Recount) {
        outcomes.push(
            RepairOutcome::new(
                "reap_orphan_part",
                RepairStatus::Skipped,
                format!(
                    "{} orphan part record(s) left alone: the report's holder set was not closed, \
                     and reaping a part releases the block references it held",
                    orphans.len()
                ),
            )
            .at(Severity::Warn),
        );
        return Ok(Vec::new());
    }

    Ok(orphans
        .into_iter()
        .map(|(storage_key, part)| RepairAction::ReapOrphanPart {
            bucket: part.bucket().to_string(),
            key: part.key().to_string(),
            upload_id: part.upload_id().to_string(),
            part_number: part.part_number(),
            storage_key,
        })
        .collect())
}

/// Part records whose upload record does not exist, each with the key it is
/// actually filed under, in the tree's own order.
///
/// The same derivation `passes::multipart_report` makes; repeated here
/// typed, because the finding it produces renders that key as report text
/// and a reap needs the bytes. Value-driven throughout (ADR 0003 hard rule
/// 4): the triple checked against `_UPLOADS` is the one the part record's
/// VALUE carries, which is what lets a legacy dash-keyed record -- whose key
/// no point read can rebuild -- be recognised and reaped at all.
///
/// Everything is collected before the caller removes anything: the reap
/// writes to the tree this iterates.
fn orphan_parts(fs: &CasFS) -> Result<Vec<(Vec<u8>, MultiPart)>, MetaError> {
    let tree = fs
        .shared_block_store()
        .meta_store()
        .get_tree_ext(MULTIPART_PARTS_TREE)?;

    let mut orphans = Vec::new();
    for item in tree.iter_all() {
        let (storage_key, raw) = item?;
        let part = MultiPart::try_from(&*raw).map_err(MetaError::from)?;
        if fs
            .get_upload(part.bucket(), part.key(), part.upload_id())?
            .is_none()
        {
            orphans.push((storage_key, part));
        }
    }
    Ok(orphans)
}

/// Object trees with no `_BUCKETS` row, sorted. The same derivation
/// `passes::bucket_integrity` makes; repeated here typed, because the
/// finding it produces states the tree name in prose.
fn stranded_trees(namespace: &MetaStore) -> Result<Vec<String>, MetaError> {
    let named: HashSet<String> = namespace.list_bucket_names()?.into_iter().collect();
    let mut stranded: Vec<String> = namespace
        .list_trees()?
        .into_iter()
        .filter(|tree| !tree.starts_with('_') && !named.contains(tree))
        .collect();
    stranded.sort();
    Ok(stranded)
}

/// Round two: everything else, planned against the store the teardowns
/// left behind, in application order.
fn plan_repairs(
    ctx: &RepairContext<'_>,
    report: &Report,
    outcomes: &mut Vec<RepairOutcome>,
) -> Result<Vec<RepairAction>, RepairError> {
    let scrub = ctx.scrub_context();
    let records = walk_records(&scrub).map_err(RepairError::Store)?;
    let disk = walk_disk(&scrub).map_err(RepairError::Disk)?;
    let root = ctx.blocks_root().to_path_buf();

    let mut actions = Vec::new();

    // 1. The corruption verdicts, from the report: this planner will not
    //    re-hash the store.
    actions.extend(plan_corrupt(report, &disk, outcomes));

    // 2. Records whose file is not where they say. Which of the two
    //    treatments each gets is the dangling sweep's classification,
    //    re-derived typed.
    let mut on_disk: HashMap<BlockId, Vec<u8>> = HashMap::new();
    for file in &disk.files {
        on_disk.entry(file.id).or_default().push(file.depth);
    }
    let mut adoptions = Vec::new();
    let mut degradings = Vec::new();
    for (id, block) in &records.records {
        if block.is_degraded() || block_disk_path(id, block.depth(), root.clone()).is_file() {
            continue;
        }
        let mut depths: Vec<u8> = on_disk
            .get(id)
            .map(|depths| {
                depths
                    .iter()
                    .copied()
                    .filter(|d| *d != block.depth())
                    .collect()
            })
            .unwrap_or_default();
        depths.sort_unstable();

        if depths.is_empty() {
            degradings.push(RepairAction::MarkDegraded { block: *id });
        } else {
            adoptions.push(RepairAction::AdoptOrphan {
                block: *id,
                record_depth: block.depth(),
                candidates: depths
                    .into_iter()
                    .map(|depth| (depth, block_disk_path(id, depth, root.clone())))
                    .collect(),
            });
        }
    }
    sort_by_block(&mut adoptions);
    sort_by_block(&mut degradings);
    actions.extend(adoptions);

    // 3. The recount, gated. Hard rule 1: with the holder set unclosed, no
    //    refcount may move in either direction.
    if report.ran(Pass::Recount) {
        let expected = expected_counts(&scrub).map_err(RepairError::Holders)?;
        let mut set_rcs = Vec::new();
        for (id, block) in &records.records {
            let holders = expected.get(id).copied().unwrap_or(0);
            let rc = block.rc() as u64;
            if rc != holders {
                set_rcs.push(RepairAction::SetRc {
                    block: *id,
                    from: rc,
                    to: holders,
                });
            }
        }
        sort_by_block(&mut set_rcs);
        actions.extend(set_rcs);
    } else {
        outcomes.push(
            RepairOutcome::new(
                "set_rc",
                RepairStatus::Skipped,
                "no refcount was touched: the report did not run the recount, so the holder set \
                 was not closed, and a count over a partial holder set would authorise freeing \
                 blocks that are still referenced",
            )
            .at(Severity::Warn),
        );
    }

    // 4. What is still fileless is unrecoverable.
    actions.extend(degradings);

    // 5. Files nothing can reference. Last, so that a file some action
    //    above still wanted is gone by the time this deletes it.
    let present: HashSet<(BlockId, u8)> = disk
        .files
        .iter()
        .map(|file| (file.id, file.depth))
        .collect();
    for file in &disk.files {
        match records.records.get(&file.id) {
            None => actions.push(RepairAction::DeleteOrphan {
                block: file.id,
                path: file.path.clone(),
            }),
            Some(block)
                if block.depth() != file.depth && present.contains(&(file.id, block.depth())) =>
            {
                actions.push(RepairAction::DeleteOffDepth {
                    block: file.id,
                    path: file.path.clone(),
                });
            }
            // Either the record's own file, or the adoption candidate
            // handled above -- one file, one action.
            Some(_) => {}
        }
    }
    for foreign in &disk.foreign {
        actions.push(RepairAction::QuarantineForeign {
            path: foreign.path.clone(),
        });
    }

    Ok(actions)
}

/// The corrupt files the report named, resolved back to the walk's typed
/// paths.
///
/// A finding renders its path lossily; the pair (block, rendered path) is
/// still exact for a block file, because every path in one walk is unique
/// and a block file's own name is hex. A finding whose file the walk no
/// longer shows is reported skipped rather than guessed at.
fn plan_corrupt(
    report: &Report,
    disk: &DiskWalk,
    outcomes: &mut Vec<RepairOutcome>,
) -> Vec<RepairAction> {
    let mut actions = Vec::new();
    for finding in report
        .findings
        .iter()
        .filter(|f| f.class == FindingClass::CorruptBlock)
    {
        let (Some(hex), Some(path)) = (finding.block.as_deref(), finding.path.as_deref()) else {
            continue;
        };
        match disk
            .files
            .iter()
            .find(|file| file.id.to_hex() == hex && file.path.to_string_lossy() == path)
        {
            Some(file) => actions.push(RepairAction::QuarantineCorrupt {
                block: file.id,
                path: file.path.clone(),
            }),
            None => outcomes.push(RepairOutcome::new(
                "quarantine_corrupt",
                RepairStatus::Skipped,
                format!("the report named a corrupt file at {path}, which is no longer there"),
            )),
        }
    }
    actions
}

/// Orders actions by block address, so two runs over one store plan and
/// report in the same order. The walks hand back hash maps.
fn sort_by_block(actions: &mut [RepairAction]) {
    actions.sort_by_key(|action| action.block().map(|id| id.to_hex()));
}

/// Whether this engine claims to repair a finding of this class.
///
/// The post-repair check is exactly this list: a class nobody repairs
/// (a holder pointing at a block with no record, a record that will not
/// decode, a size mismatch) is expected to survive and says nothing about
/// whether the repair worked.
fn is_repairable(class: FindingClass) -> bool {
    match class {
        FindingClass::RefcountOverCount
        | FindingClass::RefcountUnderCount
        | FindingClass::OrphanFile
        | FindingClass::OffDepthFile
        | FindingClass::ForeignFile
        | FindingClass::DanglingRecord
        | FindingClass::AdoptableDanglingRecord
        | FindingClass::CorruptBlock
        | FindingClass::HalfDeletedBucket
        | FindingClass::OrphanPart => true,
        FindingClass::MissingBlockRecord
        | FindingClass::UndecodableBlockRecord
        | FindingClass::SizeMismatch
        | FindingClass::DegradedRecord
        | FindingClass::MultipartUpload
        | FindingClass::HolderEnumerationFailed
        | FindingClass::PostRepairRecountDirty => false,
    }
}

/// Hard rule 2: anything repairable still standing after `--repair` makes
/// the run CRITICAL, whatever that finding's own severity is. A leftover
/// orphan file is INFO on its own; a leftover orphan file after a repair
/// that said it would delete it is a repair that did not work.
pub(super) fn flag_unfinished(post: Report) -> Report {
    let mut left: BTreeMap<FindingClass, usize> = BTreeMap::new();
    for finding in &post.findings {
        if is_repairable(finding.class) {
            *left.entry(finding.class).or_default() += 1;
        }
    }
    if left.is_empty() {
        return post;
    }

    let mut findings = post.findings;
    for (class, count) in left {
        findings.push(Finding::new(
            FindingClass::PostRepairRecountDirty,
            format!(
                "{count} {} finding(s) survived --repair, which claims to repair that class: the \
                 repair did not finish, and no report of this store is clean until it does",
                class.as_str()
            ),
        ));
    }
    Report::new(post.store, post.passes_run, findings)
}
