//! What a scrub reports: one finding per thing that is wrong, or per thing
//! an operator should see before repairing.
//!
//! The model is the tool's scripting API (ADR 0005 renders it as `--json`),
//! so the field names here are a contract, not an implementation detail.
//! Everything is owned and `Serialize`; nothing borrows the store.
//!
//! Paths are rendered lossily into `String` rather than kept as `PathBuf`:
//! serde refuses to serialize a non-UTF-8 path, and the one class of finding
//! most likely to carry a strange name is exactly the one that must always
//! make it into the report (a foreign file). The typed `PathBuf` stays on
//! the walker output, which is what repair acts on.

use std::path::Path;

use serde::Serialize;

use crate::metastore::BlockId;

/// How bad a finding is.
///
/// Ordered: `Info < Warn < Critical`, so a report can sort by severity and
/// a run's exit code is a function of the maximum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// Expected leakage, or information an operator asked for. Exit 0.
    Info,
    /// An inconsistency that is not loss and not loss-risk. Exit 1.
    Warn,
    /// Loss or loss-risk: an under-count, an unrecoverable block, corruption,
    /// a partial holder set, a dirty post-repair recount. Exit 2.
    Critical,
}

/// What kind of thing was found. One variant per class the ADR 0005 passes
/// can produce; the severity each carries by default is
/// [`FindingClass::default_severity`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingClass {
    /// The record's rc is higher than the walked holder count. The expected
    /// direction: overwrite leak, cancellation residue.
    RefcountOverCount,
    /// The record's rc is lower than the walked holder count -- a premature
    /// free waiting to happen.
    RefcountUnderCount,
    /// A holder references a block that has no record at all.
    MissingBlockRecord,
    /// A block record that does not decode.
    UndecodableBlockRecord,
    /// A block file no record references.
    OrphanFile,
    /// A file whose id has a record, but at a different depth. Unreferenced:
    /// the record's depth is where reads look.
    OffDepthFile,
    /// Something under the blocks root that is not a block file or a fanout
    /// directory of this store's layout.
    ForeignFile,
    /// A block file whose size does not match its record's.
    SizeMismatch,
    /// A record whose file is missing and cannot be adopted: the bytes are
    /// gone.
    DanglingRecord,
    /// A record whose file is missing but whose id exists elsewhere on disk,
    /// so adoption can repair it after re-hashing.
    AdoptableDanglingRecord,
    /// A record already flagged degraded: known damage awaiting a heal, not
    /// a fresh discovery.
    DegradedRecord,
    /// A block file whose bytes do not hash to the address it is filed
    /// under.
    CorruptBlock,
    /// An object tree with no `_BUCKETS` row: a bucket teardown that
    /// crashed halfway.
    HalfDeletedBucket,
    /// An in-flight multipart upload, reported so its parts are visible.
    MultipartUpload,
    /// The holder set could not be closed, so no recount was run. Nothing
    /// downstream of this is trustworthy.
    HolderEnumerationFailed,
    /// The recount that must come back clean after `--repair` did not.
    PostRepairRecountDirty,
}

impl FindingClass {
    /// The name this class carries in both renderings.
    ///
    /// Must match what serde writes; the test below pins that, so the text
    /// report and the JSON report can never drift apart.
    pub fn as_str(self) -> &'static str {
        match self {
            FindingClass::RefcountOverCount => "refcount_over_count",
            FindingClass::RefcountUnderCount => "refcount_under_count",
            FindingClass::MissingBlockRecord => "missing_block_record",
            FindingClass::UndecodableBlockRecord => "undecodable_block_record",
            FindingClass::OrphanFile => "orphan_file",
            FindingClass::OffDepthFile => "off_depth_file",
            FindingClass::ForeignFile => "foreign_file",
            FindingClass::SizeMismatch => "size_mismatch",
            FindingClass::DanglingRecord => "dangling_record",
            FindingClass::AdoptableDanglingRecord => "adoptable_dangling_record",
            FindingClass::DegradedRecord => "degraded_record",
            FindingClass::CorruptBlock => "corrupt_block",
            FindingClass::HalfDeletedBucket => "half_deleted_bucket",
            FindingClass::MultipartUpload => "multipart_upload",
            FindingClass::HolderEnumerationFailed => "holder_enumeration_failed",
            FindingClass::PostRepairRecountDirty => "post_repair_recount_dirty",
        }
    }

    /// Every class, for exhaustive tests and for a `--help` that lists them.
    pub const ALL: [FindingClass; 16] = [
        FindingClass::RefcountOverCount,
        FindingClass::RefcountUnderCount,
        FindingClass::MissingBlockRecord,
        FindingClass::UndecodableBlockRecord,
        FindingClass::OrphanFile,
        FindingClass::OffDepthFile,
        FindingClass::ForeignFile,
        FindingClass::SizeMismatch,
        FindingClass::DanglingRecord,
        FindingClass::AdoptableDanglingRecord,
        FindingClass::DegradedRecord,
        FindingClass::CorruptBlock,
        FindingClass::HalfDeletedBucket,
        FindingClass::MultipartUpload,
        FindingClass::HolderEnumerationFailed,
        FindingClass::PostRepairRecountDirty,
    ];

    /// The severity this class carries unless a pass says otherwise.
    ///
    /// Encoded here rather than at each construction site so the ADR's
    /// severity table has exactly one home.
    pub fn default_severity(self) -> Severity {
        match self {
            FindingClass::RefcountOverCount
            | FindingClass::OrphanFile
            | FindingClass::OffDepthFile
            | FindingClass::AdoptableDanglingRecord
            | FindingClass::DegradedRecord
            | FindingClass::MultipartUpload => Severity::Info,
            FindingClass::ForeignFile | FindingClass::HalfDeletedBucket => Severity::Warn,
            FindingClass::RefcountUnderCount
            | FindingClass::MissingBlockRecord
            | FindingClass::UndecodableBlockRecord
            | FindingClass::SizeMismatch
            | FindingClass::DanglingRecord
            | FindingClass::CorruptBlock
            | FindingClass::HolderEnumerationFailed
            | FindingClass::PostRepairRecountDirty => Severity::Critical,
        }
    }
}

/// Something that references a block: the blast radius of damage to it.
///
/// Keys are rendered lossily -- an object key is bytes, not necessarily
/// UTF-8, and a report that omitted the damaged object because its name is
/// strange would hide exactly what the operator needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HolderRef {
    /// An object record in a bucket tree.
    Object {
        /// Bucket tree the record lives in.
        bucket: String,
        /// Object key, rendered lossily.
        key: String,
    },
    /// A part record of an upload that has not completed.
    Part {
        /// Bucket the upload targets.
        bucket: String,
        /// Key the upload targets.
        key: String,
        /// Upload the part belongs to.
        upload_id: String,
        /// Position of the part within the upload.
        part_number: i64,
    },
}

impl std::fmt::Display for HolderRef {
    /// One line, for the text report.
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            HolderRef::Object { bucket, key } => write!(f, "object {bucket}/{key}"),
            HolderRef::Part {
                bucket,
                key,
                upload_id,
                part_number,
            } => write!(
                f,
                "part {part_number} of upload {upload_id} ({bucket}/{key})"
            ),
        }
    }
}

/// One thing the scrub found.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Finding {
    /// How bad it is.
    pub severity: Severity,
    /// What kind of thing it is.
    pub class: FindingClass,
    /// The block this is about, as lowercase hex, when it is about one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block: Option<String>,
    /// The path this is about, rendered lossily, when it is about one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// The specifics: counts, sizes, depths, decode errors. Free text, meant
    /// to be read by a person; scripts key off `class` and the fields above.
    pub evidence: String,
    /// Holders affected. Only populated where a pass enumerated them (the
    /// blast radius of a damaged block); empty otherwise.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub holders: Vec<HolderRef>,
}

impl Finding {
    /// A finding of `class` at that class's default severity.
    pub fn new(class: FindingClass, evidence: impl Into<String>) -> Self {
        Self {
            severity: class.default_severity(),
            class,
            block: None,
            path: None,
            evidence: evidence.into(),
            holders: Vec::new(),
        }
    }

    /// Names the block this finding is about.
    #[must_use]
    pub fn with_block(mut self, id: &BlockId) -> Self {
        self.block = Some(id.to_hex());
        self
    }

    /// Names the path this finding is about.
    #[must_use]
    pub fn with_path(mut self, path: &Path) -> Self {
        self.path = Some(path.to_string_lossy().into_owned());
        self
    }

    /// Attaches the holders a damaged block would take down with it.
    #[must_use]
    pub fn with_holders(mut self, holders: Vec<HolderRef>) -> Self {
        self.holders = holders;
        self
    }

    /// Overrides the class's default severity.
    #[must_use]
    pub fn at(mut self, severity: Severity) -> Self {
        self.severity = severity;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::BLOCKID_SIZE;

    #[test]
    fn severity_orders_info_below_critical() {
        assert!(Severity::Info < Severity::Warn);
        assert!(Severity::Warn < Severity::Critical);
        assert_eq!(
            [Severity::Critical, Severity::Info, Severity::Warn]
                .iter()
                .max(),
            Some(&Severity::Critical)
        );
    }

    /// The severity table of the ADR, pinned: the three directions that mean
    /// loss are CRITICAL, the expected leakage is INFO.
    #[test]
    fn default_severities_follow_the_adr() {
        assert_eq!(
            FindingClass::RefcountOverCount.default_severity(),
            Severity::Info
        );
        assert_eq!(
            FindingClass::RefcountUnderCount.default_severity(),
            Severity::Critical
        );
        assert_eq!(FindingClass::ForeignFile.default_severity(), Severity::Warn);
        assert_eq!(
            FindingClass::HalfDeletedBucket.default_severity(),
            Severity::Warn
        );
        assert_eq!(
            FindingClass::DanglingRecord.default_severity(),
            Severity::Critical
        );
        assert_eq!(
            FindingClass::DegradedRecord.default_severity(),
            Severity::Info
        );
    }

    /// The report is a machine contract: it must serialize, and the absent
    /// fields must stay absent rather than surfacing as nulls.
    #[test]
    fn findings_serialize_to_the_documented_shape() {
        let id = BlockId::from([0xabu8; BLOCKID_SIZE]);
        let finding = Finding::new(FindingClass::DanglingRecord, "no file at depth 2")
            .with_block(&id)
            .with_path(std::path::Path::new("/data/blocks/ab/abab"))
            .with_holders(vec![HolderRef::Object {
                bucket: "photos".to_string(),
                key: "cat.png".to_string(),
            }]);

        let json: serde_json::Value = serde_json::to_value(&finding).unwrap();
        assert_eq!(json["severity"], "critical");
        assert_eq!(json["class"], "dangling_record");
        assert_eq!(json["block"], id.to_hex());
        assert_eq!(json["evidence"], "no file at depth 2");
        assert_eq!(json["holders"][0]["kind"], "object");
        assert_eq!(json["holders"][0]["bucket"], "photos");

        // A finding about nothing in particular carries no empty keys.
        let bare = Finding::new(FindingClass::MultipartUpload, "3 parts, 12 MiB");
        let json: serde_json::Value = serde_json::to_value(&bare).unwrap();
        assert!(json.get("block").is_none(), "{json}");
        assert!(json.get("path").is_none(), "{json}");
        assert!(json.get("holders").is_none(), "{json}");
    }

    /// The text report and the JSON report name a class identically. Two
    /// spellings of one name is a bug waiting for a grep to miss it.
    #[test]
    fn class_names_match_in_both_renderings() {
        for class in FindingClass::ALL {
            let json = serde_json::to_value(class).unwrap();
            assert_eq!(json, serde_json::Value::String(class.as_str().to_string()));
        }
    }

    /// A part holder names the upload, not just the object: two uploads of
    /// one key are different blast radii.
    #[test]
    fn part_holders_name_their_upload() {
        let holder = HolderRef::Part {
            bucket: "b".to_string(),
            key: "k".to_string(),
            upload_id: "u-1".to_string(),
            part_number: 2,
        };
        let json: serde_json::Value = serde_json::to_value(&holder).unwrap();
        assert_eq!(json["kind"], "part");
        assert_eq!(json["upload_id"], "u-1");
        assert_eq!(json["part_number"], 2);
    }
}
