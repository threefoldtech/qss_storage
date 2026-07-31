//! Read-only queries against a store on disk.
//!
//! # Which database each subcommand opens
//!
//! One `--meta-root` holds two headered fjall databases, and they hold
//! different things:
//!
//! - `<meta_root>/db` -- the *namespace* DB. `CasFS::new` pushes `db` onto the
//!   namespace metadata path, so this is where the bucket trees live: one tree
//!   per bucket, one key per object.
//! - `<meta_root>/blocks/db` -- the *blocks* DB, built by
//!   `SharedBlockStore::new` from `meta_path.join("blocks")` with `db` pushed
//!   on top. It holds `_BLOCKS` and `_MULTIPART_PARTS`, shared by
//!   every namespace.
//!
//! So `num-keys <bucket>` counts keys in a bucket tree and must open the
//! namespace DB; `disk-space` asks fjall how much disk a database occupies,
//! which is a per-database number, so it reports both and their total. The
//! block *data* files under `<fs_root>/blocks/` are not fjall's to measure and
//! are not counted here.
//!
//! Until this module was fixed it opened `<meta_root>` itself -- neither of
//! those two paths -- which meant `num-keys` counted an empty database that
//! the tool had just created.

use std::path::{Path, PathBuf};

use anyhow::{Result, bail};

use crate::store_options::StoreOptions;
use cas_storage::metastore::store_header::{STORE_HEADER_MAGIC, StoreInit, classify_db_dir};
use cas_storage::{FjallStore, MetaStore, StorageEngine, StoreHeader};

/// Path of the namespace metadata DB under a `--meta-root`, the way
/// `CasFS::new` builds it.
fn namespace_db(meta_root: &Path) -> PathBuf {
    meta_root.join("db")
}

/// Path of the shared block metadata DB under a `--meta-root`, the way
/// `CasFS::single_namespace` plus `SharedBlockStore::new` build it.
fn blocks_db(meta_root: &Path) -> PathBuf {
    meta_root.join("blocks").join("db")
}

/// Opens an existing store at `db_path` through the header-aware path, so the
/// tools refuse a store this build may not read for the same reasons the
/// server does.
///
/// Durability, inline threshold and metadata backend all come from `store`,
/// which is the merge of the CLI flags and the config file. A tool that opened
/// the store with settings other than the ones the server runs with would
/// report on a store nobody is running.
///
/// # Errors
///
/// If there is no store at `db_path`. `MetaStore::open_or_create` would create
/// one, and a read-only query that silently creates an empty database answers
/// a question nobody asked -- `num-keys` on a mistyped path would report 0
/// rather than say the path is wrong.
fn open_existing(db_path: PathBuf, store: &StoreOptions) -> Result<(MetaStore, StoreHeader)> {
    if classify_db_dir(&db_path)? == StoreInit::Create {
        bail!("no store at {}", db_path.display());
    }

    let inline = store.inline_metadata_size;
    let spec = store.header_spec();
    let opened = match store.metadata_db {
        StorageEngine::Fjall => {
            let durability = Some(store.durability);
            MetaStore::open_or_create(db_path, inline, spec, |p| {
                FjallStore::new(p, inline, durability)
            })?
        }
    };
    Ok(opened)
}

/// Number of keys in `bucket_name`, read from the namespace DB.
///
/// # Errors
///
/// If there is no such bucket. `num_keys` on the underlying store would open
/// (and thereby create) the keyspace and report 0, so a typo'd bucket name
/// would both answer wrongly and leave a stray keyspace in the server's
/// database.
pub fn num_keys(meta_root: PathBuf, store: &StoreOptions, bucket_name: &str) -> Result<usize> {
    let db_path = namespace_db(&meta_root);
    let (meta_store, _header) = open_existing(db_path.clone(), store)?;

    // Per-tree key count is not on MetaStore; reach into the underlying Store.
    let underlying = meta_store.get_underlying_store();
    if !underlying.tree_exists(bucket_name)? {
        bail!("no bucket {bucket_name:?} in {}", db_path.display());
    }
    Ok(underlying.num_keys(bucket_name)?)
}

/// Disk occupied by the metadata databases of one `--meta-root`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskSpace {
    /// Bytes used by the namespace DB (`<meta_root>/db`).
    pub namespace: u64,
    /// Bytes used by the shared block DB (`<meta_root>/blocks/db`), `None`
    /// when the store has none -- a respd-style store is a single database.
    pub blocks: Option<u64>,
}

impl DiskSpace {
    /// Bytes used by every metadata database under the meta root.
    pub fn total(&self) -> u64 {
        self.namespace + self.blocks.unwrap_or(0)
    }
}

/// Disk used by the metadata databases under `meta_root`.
///
/// Both databases are reported: the block metadata is usually the larger of
/// the two, so a single number for either one alone reads as the store's
/// footprint while being a fraction of it.
pub fn disk_space(meta_root: PathBuf, store: &StoreOptions) -> Result<DiskSpace> {
    let (namespace, _header) = open_existing(namespace_db(&meta_root), store)?;
    let namespace = namespace.disk_space();

    let blocks_path = blocks_db(&meta_root);
    let blocks = if classify_db_dir(&blocks_path)? == StoreInit::Create {
        None
    } else {
        let (blocks, _header) = open_existing(blocks_path, store)?;
        Some(blocks.disk_space())
    };

    Ok(DiskSpace { namespace, blocks })
}

/// One database's header, with the path it was read from.
pub struct LabelledHeader {
    /// What this database is: `namespace DB` or `blocks DB`.
    pub label: &'static str,
    /// The db directory the header was read from.
    pub path: PathBuf,
    /// The header itself, already validated by the open.
    pub header: StoreHeader,
}

impl LabelledHeader {
    /// Renders the header as plain lines, one field per line.
    ///
    /// `created_at` is printed twice: humanized for a person, raw for whoever
    /// is comparing it against the bytes on disk.
    pub fn render(&self) -> String {
        let magic = String::from_utf8_lossy(&STORE_HEADER_MAGIC);
        let created = humanize(self.header.created_at());
        format!(
            "{} {}\n  magic:      {}\n  version:    {}\n  algo:       {} ({})\n  \
             width:      {}\n  created_at: {} ({})\n",
            self.label,
            self.path.display(),
            magic,
            self.header.version(),
            self.header.hasher().algo_name(),
            self.header.hash_algo(),
            self.header.hash_width(),
            created,
            self.header.created_at(),
        )
    }
}

/// Formats a unix timestamp as UTC, falling back to the raw value if it is out
/// of the range a date can be built from.
fn humanize(created_at: u64) -> String {
    match i64::try_from(created_at)
        .ok()
        .and_then(|secs| chrono::DateTime::from_timestamp(secs, 0))
    {
        Some(dt) => dt.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        None => format!("unrepresentable timestamp {created_at}"),
    }
}

/// Headers of every metadata database under `meta_root`.
///
/// The namespace DB must be there; the blocks DB is reported only if it
/// exists, since a store written by respd has a single database and no block
/// metadata at all.
pub fn headers(meta_root: PathBuf, store: &StoreOptions) -> Result<Vec<LabelledHeader>> {
    let mut out = Vec::with_capacity(2);

    let path = namespace_db(&meta_root);
    let (_store, header) = open_existing(path.clone(), store)?;
    out.push(LabelledHeader {
        label: "namespace DB",
        path,
        header,
    });

    let path = blocks_db(&meta_root);
    if classify_db_dir(&path)? == StoreInit::Open {
        let (_store, header) = open_existing(path.clone(), store)?;
        out.push(LabelledHeader {
            label: "blocks DB",
            path,
            header,
        });
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cas_storage::{CasFS, Durability, Hasher, SharedMetrics};
    use tempfile::TempDir;

    fn options() -> StoreOptions {
        StoreOptions {
            metadata_db: StorageEngine::Fjall,
            durability: Durability::Buffer,
            inline_metadata_size: Some(1024),
            verify_on_read: false,
            hasher: Hasher::Blake3W32,
        }
    }

    /// Writes `keys` inlined objects into one bucket the way the server does,
    /// then drops the `CasFS` -- fjall holds a directory lock, so the store
    /// has to be closed before a tool can open it.
    fn store_with_keys(bucket: &str, keys: usize) -> TempDir {
        let dir = TempDir::new().unwrap();
        let opts = options();
        let casfs = CasFS::single_namespace(
            dir.path().to_path_buf(),
            dir.path().to_path_buf(),
            SharedMetrics::default(),
            opts.metadata_db,
            opts.inline_metadata_size,
            Some(opts.durability),
            Some(opts.header_spec()),
            false,
        )
        .unwrap();
        casfs.create_bucket(bucket).unwrap();
        for i in 0..keys {
            casfs
                .store_inlined_object(bucket, &format!("key-{i}"), b"payload".to_vec())
                .unwrap();
        }
        drop(casfs);
        dir
    }

    /// The regression this module was fixed for: the count has to be the one
    /// the writer wrote, not 0 from a database the tool created itself.
    #[test]
    fn num_keys_sees_what_casfs_wrote() {
        let dir = store_with_keys("testbucket", 3);
        let count = num_keys(dir.path().to_path_buf(), &options(), "testbucket").unwrap();
        assert_eq!(count, 3);
    }

    /// A path with no store is an error, not an empty store silently created.
    #[test]
    fn a_missing_store_is_refused() {
        let dir = TempDir::new().unwrap();
        let err = num_keys(dir.path().to_path_buf(), &options(), "testbucket").unwrap_err();
        assert!(err.to_string().contains("no store at"), "{err}");
    }

    /// Likewise a bucket that is not there: saying 0 would be indistinguishable
    /// from an empty bucket, and counting it would create the keyspace.
    #[test]
    fn a_missing_bucket_is_refused() {
        let dir = store_with_keys("testbucket", 1);
        let err = num_keys(dir.path().to_path_buf(), &options(), "typo").unwrap_err();
        assert!(err.to_string().contains("no bucket \"typo\""), "{err}");
    }

    #[test]
    fn disk_space_covers_both_databases() {
        let dir = store_with_keys("testbucket", 3);
        let space = disk_space(dir.path().to_path_buf(), &options()).unwrap();
        assert!(space.namespace > 0);
        let blocks = space.blocks.expect("a CasFS store has a blocks DB");
        assert_eq!(space.total(), space.namespace + blocks);
    }

    #[test]
    fn headers_reports_both_databases() {
        let dir = store_with_keys("testbucket", 1);
        let headers = headers(dir.path().to_path_buf(), &options()).unwrap();

        let labels: Vec<_> = headers.iter().map(|h| h.label).collect();
        assert_eq!(labels, vec!["namespace DB", "blocks DB"]);
        for entry in &headers {
            assert_eq!(entry.header.hasher(), Hasher::Blake3W32);
            assert_eq!(
                entry.header.version(),
                cas_storage::metastore::store_header::STORE_HEADER_VERSION
            );
            let text = entry.render();
            assert!(text.contains("magic:      QSST"), "{text}");
            assert!(text.contains("algo:       blake3 (1)"), "{text}");
            assert!(text.contains("width:      32"), "{text}");
        }
    }

    /// The header the tool prints is the one the store was created with, not
    /// the one the config happens to ask for.
    #[test]
    fn headers_report_the_stores_own_width() {
        let dir = TempDir::new().unwrap();
        let mut opts = options();
        opts.hasher = Hasher::Blake3W16;
        let casfs = CasFS::single_namespace(
            dir.path().to_path_buf(),
            dir.path().to_path_buf(),
            SharedMetrics::default(),
            opts.metadata_db,
            opts.inline_metadata_size,
            Some(opts.durability),
            Some(opts.header_spec()),
            false,
        )
        .unwrap();
        drop(casfs);

        // Asked with the default width 32; the header on disk says 16.
        let headers = headers(dir.path().to_path_buf(), &options()).unwrap();
        assert_eq!(headers.len(), 2);
        for entry in &headers {
            assert_eq!(entry.header.hasher(), Hasher::Blake3W16);
            assert!(entry.render().contains("width:      16"));
        }
    }

    #[test]
    fn timestamps_are_humanized_as_utc() {
        assert_eq!(humanize(0), "1970-01-01T00:00:00Z");
        assert_eq!(humanize(1_753_876_496), "2025-07-30T11:54:56Z");
    }
}
