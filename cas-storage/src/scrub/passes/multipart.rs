//! Pass 5: the in-flight multipart uploads, and the age rendering it needs.

use std::collections::HashMap;

use chrono::Utc;

use crate::cas::multipart::MultiPart;
use crate::metastore::{MULTIPART_PARTS_TREE, MetaError, UPLOADS_TREE, UploadRecord};
use crate::scrub::ScrubContext;
use crate::scrub::findings::{Finding, FindingClass};
use crate::scrub::holders::HolderEnumerationError;

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
pub(super) fn humanize_age(seconds: i64) -> String {
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
