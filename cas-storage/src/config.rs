//! `qss_storage.toml`: the file that configures a deployment.
//!
//! The file is the primary configuration source. CLI flags override it field
//! by field, and a built-in default fills in whatever neither supplies:
//!
//! ```text
//! CLI flag  >  config file  >  built-in default
//! ```
//!
//! Which file is read, in order:
//!
//! 1. the path given to `--config` -- must exist, and a parse error there is
//!    fatal rather than a reason to fall through to the next candidate;
//! 2. `./qss_storage.toml`;
//! 3. `/etc/qss_storage/qss_storage.toml`;
//! 4. nothing, i.e. [`QssStorageConfig::default`].
//!
//! # Why every leaf is an `Option`
//!
//! "Absent from the file" and "present and set to the default" have to be
//! distinguishable, otherwise the merge cannot tell a config value from a
//! placeholder and the CLI-over-file precedence collapses. So no leaf field
//! carries a serde default: the defaults live in the `DEFAULT_*` constants
//! below and are applied once, at merge time, in each binary. That keeps one
//! source of truth per default -- the constant -- rather than one in clap, one
//! in serde and one in the code that consumes the value.
//!
//! # Unknown keys are an error
//!
//! Every struct here is `deny_unknown_fields`. A misspelled key in a config
//! file is otherwise silently ignored, and a storage daemon that silently
//! ignores `verify_on_reads = true` is worse than one that refuses to start.
//!
//! # `store.hash` and existing stores
//!
//! `store.hash` is consulted only when a store is *created*. On open, the
//! store header wins, because the blocks on disk are already addressed by what
//! it says (ADR 0001: the header is immutable). A config that disagrees with
//! the header of the store it opened is a warning at startup, not a refusal
//! and not a re-address.

use std::fmt::{self, Display, Formatter};
use std::io;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::cas::StorageEngine;
use crate::hasher::Hasher;
use crate::metastore::Durability;

/// File name looked for in the working directory.
pub const CONFIG_FILE_NAME: &str = "qss_storage.toml";

/// System-wide fallback location.
pub const SYSTEM_CONFIG_PATH: &str = "/etc/qss_storage/qss_storage.toml";

/// The only block hash algorithm this build accepts in `store.hash.algo`.
pub const DEFAULT_HASH_ALGO: &str = "blake3";

/// Block address width in bytes for a store created without a configured one.
/// 32 is the default; 16 is the trusted-tenant option (ADR 0002).
pub const DEFAULT_HASH_WIDTH: u8 = 32;

/// Durability for a store whose config and flags say nothing: full fsync, the
/// strongest of the three.
pub const DEFAULT_DURABILITY: Durability = Durability::Fsync;

/// Metadata backend used when nothing selects one.
pub const DEFAULT_METADATA_DB: StorageEngine = StorageEngine::Fjall;

/// Block verification on read is off unless asked for: it costs a full buffer
/// plus a re-hash per block.
pub const DEFAULT_VERIFY_ON_READ: bool = false;

/// Per-block lock stripes a store gets when nothing configures a count.
///
/// Re-exported from `cas::stripes` rather than restated, so this and the
/// number the store actually uses are one value.
pub const DEFAULT_STRIPE_COUNT: usize = crate::cas::stripes::DEFAULT_STRIPE_COUNT;

/// Largest configurable stripe count. Above this the store would allocate
/// stripes its two-byte index can never reach; see `cas::stripes`.
pub const MAX_STRIPE_COUNT: usize = crate::cas::stripes::MAX_STRIPE_COUNT;

/// Address the S3 server binds by default.
pub const DEFAULT_S3_HOST: &str = "localhost";

/// Port the S3 server binds by default.
pub const DEFAULT_S3_PORT: u16 = 8014;

/// Address the prometheus endpoint binds by default.
pub const DEFAULT_METRICS_HOST: &str = "localhost";

/// Port the prometheus endpoint binds by default.
pub const DEFAULT_METRICS_PORT: u16 = 9100;

/// Address respd binds by default.
pub const DEFAULT_RESP_HOST: &str = "127.0.0.1";

/// Port respd binds by default (the Redis port).
pub const DEFAULT_RESP_PORT: u16 = 6379;

/// Data directory respd uses by default.
pub const DEFAULT_RESP_DATA_DIR: &str = "./data";

/// respd inlines every value it can: a 1 byte threshold means "inline
/// everything that fits", which is how respd has always run.
pub const DEFAULT_RESP_INLINE_METADATA_SIZE: usize = 1;

/// Days an unfinished multipart upload survives before the stale-upload GC
/// aborts it (ADR 0003). Seven days is the conventional S3 lifecycle value and
/// is long enough that no legitimate transfer, however slow or often retried,
/// is reaped underneath a client. `0` disables the sweep.
pub const DEFAULT_MULTIPART_STALE_TTL_DAYS: u64 = 7;

/// A parsed `qss_storage.toml`.
///
/// Both service sections are optional: an s3cas-only deployment has no
/// `[resp]` table and vice versa. `[store]` is not optional because every
/// binary opens a store, but an absent table is the same as an empty one.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QssStorageConfig {
    /// Settings shared by every store this deployment opens.
    #[serde(default)]
    pub store: StoreConfig,
    /// s3cas-only settings.
    pub s3: Option<S3Config>,
    /// Multipart upload lifecycle settings. s3cas-only, but its own table
    /// rather than a sub-table of `[s3]`: what it configures is a property of
    /// the STORE (how long abandoned uploads keep their blocks), and fsck
    /// reports against the same ages.
    pub multipart: Option<MultipartConfig>,
    /// respd-only settings.
    pub resp: Option<RespConfig>,
}

/// The `[multipart]` table: the stale-upload garbage collector (ADR 0003).
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MultipartConfig {
    /// Age at which an unfinished multipart upload is aborted and its blocks
    /// released, in days. `0` disables the sweep entirely, which leaves every
    /// abandoned upload holding its blocks until an operator runs fsck.
    pub stale_ttl_days: Option<u64>,
}

/// The `[store]` table: how a store is created and opened.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StoreConfig {
    /// Block addressing. Applies at store creation only.
    #[serde(default)]
    pub hash: HashConfig,
    /// Write durability of the metadata database.
    pub durability: Option<Durability>,
    /// Objects at or below this size are stored inside their metadata record
    /// instead of as blocks. Absent means the backend default.
    pub inline_metadata_size: Option<usize>,
    /// Metadata database backend.
    pub metadata_db: Option<StorageEngine>,
    /// Re-hash whole blocks on read and refuse a block whose bytes no longer
    /// match its address.
    pub verify_on_read: Option<bool>,
    /// Number of per-block lock stripes the store's block records are
    /// serialized on (ADR 0006). Absent means [`DEFAULT_STRIPE_COUNT`].
    ///
    /// Sizing: with K concurrent block writers and N stripes, the chance a
    /// writer is spuriously serialized behind an unrelated block is about
    /// `(K-1)/N`, so N wants to be roughly 16x the peak concurrent block
    /// writers. Purely a concurrency knob -- nothing about it is written to
    /// disk, so two processes may open the same store with different counts
    /// (each still serializes its own writers correctly).
    pub stripe_count: Option<usize>,
}

/// Refuses a stripe count the store cannot honour as written.
///
/// Applied to the config-file value at parse time and to the merged
/// CLI-over-file value at resolve time, because both can be wrong and the
/// merge is where the value that will actually be used first exists.
///
/// Zero is refused rather than clamped: `Stripes::new` would turn it into a
/// single lock, which serializes every block writer in the process against
/// every other -- a silent collapse of the parallelism the whole striping
/// scheme exists for. Anything above [`MAX_STRIPE_COUNT`] is refused for the
/// mirror-image reason: the stripe index comes from two bytes of the block
/// hash, so the extra stripes are allocated and never taken, and an operator
/// who asked for 100000 would get 65536 without being told.
///
/// # Errors
///
/// [`ConfigError::UnsupportedStripeCount`] naming the offending value.
pub fn validate_stripe_count(count: usize) -> Result<(), ConfigError> {
    if count == 0 || count > MAX_STRIPE_COUNT {
        return Err(ConfigError::UnsupportedStripeCount(count));
    }
    Ok(())
}

/// The `[store.hash]` table: which hash addresses this store's blocks.
///
/// Written into the store header at creation and immutable afterwards, so
/// changing it only affects stores created from then on.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HashConfig {
    /// Algorithm name. Only `blake3` exists today; the field is here so a
    /// second algorithm does not need a schema change.
    pub algo: Option<String>,
    /// Block address width in bytes: 16 or 32.
    pub width: Option<u8>,
}

impl HashConfig {
    /// Resolves this table to the hash function new stores should use,
    /// applying [`DEFAULT_HASH_ALGO`] and [`DEFAULT_HASH_WIDTH`] where the
    /// file said nothing.
    ///
    /// # Errors
    ///
    /// [`ConfigError::UnknownHashAlgo`] or [`ConfigError::UnsupportedHashWidth`]
    /// naming the offending value. The loader calls this eagerly, so a store
    /// is never created from a config whose hash section does not resolve.
    pub fn hasher(&self) -> Result<Hasher, ConfigError> {
        let algo = self.algo.as_deref().unwrap_or(DEFAULT_HASH_ALGO);
        if !algo.eq_ignore_ascii_case(DEFAULT_HASH_ALGO) {
            return Err(ConfigError::UnknownHashAlgo(algo.to_string()));
        }
        match self.width.unwrap_or(DEFAULT_HASH_WIDTH) {
            16 => Ok(Hasher::Blake3W16),
            32 => Ok(Hasher::Blake3W32),
            other => Err(ConfigError::UnsupportedHashWidth(other)),
        }
    }
}

/// The `[s3]` table: s3cas server settings.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    /// Address the S3 endpoint binds.
    pub host: Option<String>,
    /// Port the S3 endpoint binds.
    pub port: Option<u16>,
    /// S3 access key. Authentication is mandatory, so this and `secret_key`
    /// must come from either the file or the CLI.
    pub access_key: Option<String>,
    /// S3 secret key.
    pub secret_key: Option<String>,
    /// The `[s3.metrics]` sub-table.
    pub metrics: Option<MetricsConfig>,
}

/// The `[s3.metrics]` table: where the prometheus endpoint listens.
///
/// Consumed by s3cas only; respd exports no metrics.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    /// Address the metrics endpoint binds.
    pub host: Option<String>,
    /// Port the metrics endpoint binds.
    pub port: Option<u16>,
}

/// The `[resp]` table: respd server settings.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RespConfig {
    /// Address respd binds.
    pub host: Option<String>,
    /// Port respd binds.
    pub port: Option<u16>,
    /// Directory holding respd's database.
    pub data_dir: Option<PathBuf>,
    /// Password required for admin commands. Absent means every connection is
    /// granted admin privileges.
    pub admin_password: Option<String>,
}

/// Loads the configuration, returning it together with the file it came from.
///
/// The returned path is `None` when no file was found and the defaults are in
/// force; binaries log it at startup so an operator can see which file (if
/// any) is actually in effect.
///
/// # Errors
///
/// See [`ConfigError`]. An `explicit` path that does not exist is an error
/// rather than a fall-through: a `--config` that is silently ignored is how a
/// server ends up running on a configuration nobody wrote.
pub fn load(explicit: Option<&Path>) -> Result<(QssStorageConfig, Option<PathBuf>), ConfigError> {
    let candidates = default_candidates();
    match select_path(explicit, &candidates)? {
        Some(path) => {
            let config = load_file(&path)?;
            Ok((config, Some(path)))
        }
        None => Ok((QssStorageConfig::default(), None)),
    }
}

/// The search path used when `--config` was not given: working directory
/// first, then the system-wide location.
pub fn default_candidates() -> Vec<PathBuf> {
    vec![
        PathBuf::from(CONFIG_FILE_NAME),
        PathBuf::from(SYSTEM_CONFIG_PATH),
    ]
}

/// Picks the config file to read: `explicit` if given, else the first
/// `candidates` entry that exists, else `None`.
///
/// Split out from [`load`] so the precedence can be tested against temporary
/// directories without any test having to change the process working
/// directory, which is a process-global that would make the test suite
/// order-dependent.
///
/// # Errors
///
/// [`ConfigError::NotFound`] if `explicit` does not exist.
pub fn select_path(
    explicit: Option<&Path>,
    candidates: &[PathBuf],
) -> Result<Option<PathBuf>, ConfigError> {
    if let Some(path) = explicit {
        if !path.exists() {
            return Err(ConfigError::NotFound(path.to_path_buf()));
        }
        return Ok(Some(path.to_path_buf()));
    }
    Ok(candidates.iter().find(|path| path.exists()).cloned())
}

/// Reads and parses one config file.
///
/// # Errors
///
/// [`ConfigError::Read`] if the file cannot be read, [`ConfigError::Parse`]
/// if it is not valid TOML or holds an unknown key.
pub fn load_file(path: &Path) -> Result<QssStorageConfig, ConfigError> {
    let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    parse(&text, path)
}

/// Parses config text. `path` is used only for error messages.
///
/// Validates `store.hash` and `store.stripe_count` eagerly so a bad algorithm,
/// width or stripe count is reported at startup, next to the file that holds
/// it, rather than at the first store creation.
///
/// # Errors
///
/// [`ConfigError::Parse`], [`ConfigError::UnknownHashAlgo`],
/// [`ConfigError::UnsupportedHashWidth`] or
/// [`ConfigError::UnsupportedStripeCount`].
pub fn parse(text: &str, path: &Path) -> Result<QssStorageConfig, ConfigError> {
    let config: QssStorageConfig = toml::from_str(text).map_err(|source| ConfigError::Parse {
        path: path.to_path_buf(),
        source: Box::new(source),
    })?;
    config.store.hash.hasher()?;
    if let Some(count) = config.store.stripe_count {
        validate_stripe_count(count)?;
    }
    Ok(config)
}

/// Why a configuration could not be loaded.
#[derive(Debug)]
pub enum ConfigError {
    /// A `--config` path that does not exist.
    NotFound(PathBuf),
    /// The file exists but could not be read.
    Read {
        /// The file that could not be read.
        path: PathBuf,
        /// The underlying I/O error.
        source: io::Error,
    },
    /// The file is not valid TOML, or holds a key or value this build does not
    /// accept. The wrapped error carries TOML's line and column context.
    Parse {
        /// The file that failed to parse.
        path: PathBuf,
        /// TOML's error, including the offending span.
        source: Box<toml::de::Error>,
    },
    /// `store.hash.algo` names an algorithm this build does not have.
    UnknownHashAlgo(String),
    /// `store.hash.width` is not a width the algorithm supports.
    UnsupportedHashWidth(u8),
    /// `store.stripe_count` (or `--stripe-count`) is outside the usable range.
    UnsupportedStripeCount(usize),
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            ConfigError::NotFound(path) => {
                write!(f, "config file not found: {}", path.display())
            }
            ConfigError::Read { path, source } => {
                write!(f, "cannot read config file {}: {source}", path.display())
            }
            ConfigError::Parse { path, source } => {
                write!(f, "invalid config file {}:\n{source}", path.display())
            }
            ConfigError::UnknownHashAlgo(algo) => write!(
                f,
                "unknown block hash algorithm \"{algo}\" in store.hash.algo \
                 (only \"{DEFAULT_HASH_ALGO}\" is supported)"
            ),
            ConfigError::UnsupportedHashWidth(width) => write!(
                f,
                "unsupported block hash width {width} in store.hash.width \
                 (expected 16 or 32)"
            ),
            ConfigError::UnsupportedStripeCount(count) => write!(
                f,
                "unsupported stripe count {count} in store.stripe_count \
                 (expected 1 to {MAX_STRIPE_COUNT}: 0 would serialize every \
                  block writer on one lock, and the stripe index is two bytes \
                  wide so anything larger is never reached)"
            ),
        }
    }
}

/// Deliberately without a `source()` override: the `Display` above already
/// carries the underlying I/O or TOML message, and a reporter that walks the
/// chain -- anyhow, which both binaries use -- would otherwise print the same
/// parse error twice, once as the message and once as its cause.
impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every key the schema has, so a field added without a parse arm shows up
    /// here as a compile or assertion failure.
    const FULL: &str = r#"
[store]
durability = "fdatasync"
inline_metadata_size = 4096
metadata_db = "fjall"
verify_on_read = true
stripe_count = 4096

[store.hash]
algo = "blake3"
width = 16

[s3]
host = "0.0.0.0"
port = 9000
access_key = "AK"
secret_key = "SK"

[s3.metrics]
host = "127.0.0.1"
port = 9101

[multipart]
stale_ttl_days = 14

[resp]
host = "0.0.0.0"
port = 6380
data_dir = "/var/lib/respd"
admin_password = "hunter2"
"#;

    fn parse_str(text: &str) -> Result<QssStorageConfig, ConfigError> {
        parse(text, Path::new("qss_storage.toml"))
    }

    #[test]
    fn full_file_parses() {
        let config = parse_str(FULL).expect("full file must parse");

        assert_eq!(config.store.durability, Some(Durability::Fdatasync));
        assert_eq!(config.store.inline_metadata_size, Some(4096));
        assert_eq!(config.store.metadata_db, Some(StorageEngine::Fjall));
        assert_eq!(config.store.verify_on_read, Some(true));
        assert_eq!(config.store.stripe_count, Some(4096));
        assert_eq!(config.store.hash.algo.as_deref(), Some("blake3"));
        assert_eq!(config.store.hash.width, Some(16));
        assert_eq!(config.store.hash.hasher().unwrap(), Hasher::Blake3W16);

        let s3 = config.s3.expect("[s3] must parse");
        assert_eq!(s3.host.as_deref(), Some("0.0.0.0"));
        assert_eq!(s3.port, Some(9000));
        assert_eq!(s3.access_key.as_deref(), Some("AK"));
        assert_eq!(s3.secret_key.as_deref(), Some("SK"));
        let metrics = s3.metrics.expect("[s3.metrics] must parse");
        assert_eq!(metrics.host.as_deref(), Some("127.0.0.1"));
        assert_eq!(metrics.port, Some(9101));

        let multipart = config.multipart.expect("[multipart] must parse");
        assert_eq!(multipart.stale_ttl_days, Some(14));

        let resp = config.resp.expect("[resp] must parse");
        assert_eq!(resp.host.as_deref(), Some("0.0.0.0"));
        assert_eq!(resp.port, Some(6380));
        assert_eq!(resp.data_dir, Some(PathBuf::from("/var/lib/respd")));
        assert_eq!(resp.admin_password.as_deref(), Some("hunter2"));
    }

    /// The shipped example is the documentation of this schema, so it has to
    /// parse, and the values it presents as the defaults have to be the
    /// defaults. This is the test that fails when a key is renamed here and
    /// not there.
    #[test]
    fn the_example_file_parses_and_documents_the_defaults() {
        let text = include_str!("../../qss_storage.toml.example");
        let config = parse(text, Path::new("qss_storage.toml.example"))
            .expect("the shipped example must parse");

        assert_eq!(config.store.durability, Some(DEFAULT_DURABILITY));
        assert_eq!(config.store.metadata_db, Some(DEFAULT_METADATA_DB));
        assert_eq!(config.store.verify_on_read, Some(DEFAULT_VERIFY_ON_READ));
        assert_eq!(config.store.stripe_count, Some(DEFAULT_STRIPE_COUNT));
        assert_eq!(config.store.hash.algo.as_deref(), Some(DEFAULT_HASH_ALGO));
        assert_eq!(config.store.hash.width, Some(DEFAULT_HASH_WIDTH));

        let s3 = config.s3.expect("the example must show [s3]");
        assert_eq!(s3.host.as_deref(), Some(DEFAULT_S3_HOST));
        assert_eq!(s3.port, Some(DEFAULT_S3_PORT));
        let metrics = s3.metrics.expect("the example must show [s3.metrics]");
        assert_eq!(metrics.host.as_deref(), Some(DEFAULT_METRICS_HOST));
        assert_eq!(metrics.port, Some(DEFAULT_METRICS_PORT));

        let multipart = config.multipart.expect("the example must show [multipart]");
        assert_eq!(
            multipart.stale_ttl_days,
            Some(DEFAULT_MULTIPART_STALE_TTL_DAYS)
        );

        let resp = config.resp.expect("the example must show [resp]");
        assert_eq!(resp.host.as_deref(), Some(DEFAULT_RESP_HOST));
        assert_eq!(resp.port, Some(DEFAULT_RESP_PORT));
        assert_eq!(resp.data_dir, Some(PathBuf::from(DEFAULT_RESP_DATA_DIR)));
    }

    #[test]
    fn empty_file_is_the_default_config() {
        assert_eq!(parse_str("").unwrap(), QssStorageConfig::default());
        assert_eq!(
            QssStorageConfig::default()
                .store
                .hash
                .hasher()
                .expect("the default hash section must resolve"),
            Hasher::Blake3W32
        );
    }

    #[test]
    fn absent_keys_stay_none() {
        let config = parse_str("[store]\nverify_on_read = true\n\n[resp]\nport = 6380\n").unwrap();

        // Set in the file.
        assert_eq!(config.store.verify_on_read, Some(true));
        assert_eq!(config.resp.as_ref().unwrap().port, Some(6380));

        // Absent from the file: None, not a default, so a CLI flag can win and
        // the built-in default applies when it does not.
        assert_eq!(config.store.durability, None);
        assert_eq!(config.store.metadata_db, None);
        assert_eq!(config.store.inline_metadata_size, None);
        assert_eq!(config.store.stripe_count, None);
        assert_eq!(config.store.hash.width, None);
        assert_eq!(config.resp.as_ref().unwrap().host, None);
        assert_eq!(config.resp.as_ref().unwrap().data_dir, None);
        assert!(config.s3.is_none());
        assert!(config.multipart.is_none());
    }

    #[test]
    fn unknown_top_level_key_is_an_error() {
        let err = parse_str("[storage]\ndurability = \"fsync\"\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("storage"), "message must name the key: {msg}");
        assert!(
            msg.contains("qss_storage.toml"),
            "message must name the file: {msg}"
        );
    }

    #[test]
    fn unknown_nested_key_is_an_error() {
        let err = parse_str("[store]\nverify_on_reads = true\n").unwrap_err();
        assert!(
            err.to_string().contains("verify_on_reads"),
            "message must name the key: {err}"
        );

        let err = parse_str("[s3.metrics]\nbind = \"127.0.0.1\"\n").unwrap_err();
        assert!(
            err.to_string().contains("bind"),
            "message must name the key: {err}"
        );

        let err = parse_str("[multipart]\nstale_ttl = 7\n").unwrap_err();
        assert!(
            err.to_string().contains("stale_ttl"),
            "message must name the key: {err}"
        );
    }

    /// Zero is a legal value, not a missing one: it is how an operator turns
    /// the sweep off, and it must be distinguishable from an absent key (which
    /// takes the 7 day default).
    #[test]
    fn a_zero_multipart_ttl_parses_as_a_value() {
        let config = parse_str("[multipart]\nstale_ttl_days = 0\n").unwrap();
        assert_eq!(config.multipart.unwrap().stale_ttl_days, Some(0));

        let config = parse_str("[multipart]\n").unwrap();
        assert_eq!(config.multipart.unwrap().stale_ttl_days, None);
    }

    #[test]
    fn bad_durability_is_an_error() {
        let err = parse_str("[store]\ndurability = \"sync\"\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown durability option: sync"), "{msg}");
        assert!(
            msg.contains("fdatasync"),
            "message must list the options: {msg}"
        );
    }

    #[test]
    fn bad_metadata_db_is_an_error() {
        let err = parse_str("[store]\nmetadata_db = \"sled\"\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown storage engine: sled"), "{msg}");
        assert!(
            msg.contains("fjall"),
            "message must list the options: {msg}"
        );
    }

    /// The backend was removed by ADR 0007; the config value must fail loudly
    /// and the message must carry the migration path, not just "unknown".
    #[test]
    fn removed_fjall_notx_is_rejected_with_the_migration_path() {
        let err = parse_str("[store]\nmetadata_db = \"fjall_notx\"\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("removed"), "message must say removed: {msg}");
        assert!(
            msg.contains("durability") && msg.contains("buffer"),
            "message must name the migration path: {msg}"
        );
    }

    /// The two ends of the usable range are refused at parse, next to the file
    /// that holds them. Both failures are silent otherwise: zero collapses to
    /// one global lock and anything larger than the two-byte index is
    /// allocated and never taken, so an operator gets neither what they asked
    /// for nor a complaint.
    #[test]
    fn a_stripe_count_outside_the_usable_range_is_an_error() {
        let err = parse_str("[store]\nstripe_count = 0\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains('0'), "message must name the value: {msg}");
        assert!(
            msg.contains(&MAX_STRIPE_COUNT.to_string()),
            "message must name the ceiling: {msg}"
        );

        let too_many = MAX_STRIPE_COUNT + 1;
        let err = parse_str(&format!("[store]\nstripe_count = {too_many}\n")).unwrap_err();
        assert!(
            err.to_string().contains(&too_many.to_string()),
            "message must name the value: {err}"
        );

        // The boundaries themselves are legal.
        for count in [1, DEFAULT_STRIPE_COUNT, MAX_STRIPE_COUNT] {
            let config = parse_str(&format!("[store]\nstripe_count = {count}\n"))
                .unwrap_or_else(|e| panic!("{count} must be accepted: {e}"));
            assert_eq!(config.store.stripe_count, Some(count));
        }
    }

    /// The configured default and the one the store actually uses are one
    /// value, not two that happen to match today.
    #[test]
    fn the_default_stripe_count_is_the_stores_own() {
        assert_eq!(
            DEFAULT_STRIPE_COUNT,
            crate::cas::stripes::DEFAULT_STRIPE_COUNT
        );
        validate_stripe_count(DEFAULT_STRIPE_COUNT)
            .expect("the built-in default must itself be a legal value");
    }

    #[test]
    fn bad_hash_algo_is_an_error() {
        let err = parse_str("[store.hash]\nalgo = \"md5\"\n").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("md5"),
            "message must name the algorithm: {msg}"
        );
        assert!(
            msg.contains("blake3"),
            "message must name the option: {msg}"
        );
    }

    #[test]
    fn bad_hash_width_is_an_error() {
        let err = parse_str("[store.hash]\nwidth = 24\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("24"), "message must name the width: {msg}");
        assert!(
            msg.contains("16 or 32"),
            "message must list the widths: {msg}"
        );
    }

    #[test]
    fn parse_error_carries_file_and_position() {
        let err = parse_str("[store\ndurability = \"fsync\"\n").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("qss_storage.toml"),
            "message must name the file: {msg}"
        );
        // toml renders a span; the line number is what an operator needs.
        assert!(
            msg.contains("1") || msg.contains("line"),
            "message must locate the error: {msg}"
        );
    }

    #[test]
    fn explicit_path_wins_over_the_candidates() {
        let dir = tempfile::tempdir().unwrap();
        let explicit = dir.path().join("custom.toml");
        let cwd_file = dir.path().join(CONFIG_FILE_NAME);
        std::fs::write(&explicit, "").unwrap();
        std::fs::write(&cwd_file, "").unwrap();

        let picked = select_path(Some(&explicit), std::slice::from_ref(&cwd_file))
            .unwrap()
            .unwrap();
        assert_eq!(picked, explicit);
    }

    #[test]
    fn missing_explicit_path_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope.toml");

        let err = select_path(Some(&missing), &[]).unwrap_err();
        assert!(matches!(err, ConfigError::NotFound(_)));
        assert!(
            err.to_string().contains("nope.toml"),
            "message must name the path: {err}"
        );
    }

    #[test]
    fn first_existing_candidate_wins() {
        let dir = tempfile::tempdir().unwrap();
        let cwd_file = dir.path().join(CONFIG_FILE_NAME);
        let system_file = dir.path().join("etc-qss_storage.toml");
        std::fs::write(&system_file, "").unwrap();

        // Only the system file exists.
        let candidates = vec![cwd_file.clone(), system_file.clone()];
        assert_eq!(
            select_path(None, &candidates).unwrap(),
            Some(system_file.clone())
        );

        // Once the working-directory file exists it takes precedence.
        std::fs::write(&cwd_file, "").unwrap();
        assert_eq!(select_path(None, &candidates).unwrap(), Some(cwd_file));
    }

    #[test]
    fn no_candidate_means_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let candidates = vec![
            dir.path().join(CONFIG_FILE_NAME),
            dir.path().join("etc-qss_storage.toml"),
        ];
        assert_eq!(select_path(None, &candidates).unwrap(), None);
    }

    #[test]
    fn load_reads_the_explicit_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("custom.toml");
        std::fs::write(&path, "[store]\ndurability = \"buffer\"\n").unwrap();

        let (config, source) = load(Some(&path)).unwrap();
        assert_eq!(config.store.durability, Some(Durability::Buffer));
        assert_eq!(source, Some(path));
    }

    #[test]
    fn durability_and_engine_round_trip_through_display() {
        for durability in [Durability::Buffer, Durability::Fsync, Durability::Fdatasync] {
            let text = format!("[store]\ndurability = \"{durability}\"\n");
            assert_eq!(parse_str(&text).unwrap().store.durability, Some(durability));
        }
        // One engine since ADR 0007 removed the other; make this a loop
        // again when a second variant lands.
        let engine = StorageEngine::Fjall;
        let text = format!("[store]\nmetadata_db = \"{engine}\"\n");
        assert_eq!(parse_str(&text).unwrap().store.metadata_db, Some(engine));
    }
}
