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
/// stronger of the two levels ADR 0010 left standing.
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

/// Blocks one transaction carries when nothing configures a cap (ADR 0010).
///
/// Re-exported from `cas::write_path` rather than restated, so this and the
/// number the write path actually batches are one value.
pub const DEFAULT_MAX_BLOCKS_PER_COMMIT: usize =
    crate::cas::write_path::DEFAULT_MAX_BLOCKS_PER_COMMIT;

/// Cross-request group commit (ADR 0011) is OFF unless an operator asks for
/// it.
///
/// Grouping strangers couples their fates and widens what one commit carries.
/// A store that never sets this runs the ADR 0010 write path byte for byte.
pub const DEFAULT_GROUP_COMMIT: bool = false;

/// Extra wait a commit station spends gathering members after the first one
/// arrives (ADR 0011), when nothing configures one.
///
/// Zero, and zero means the timer does not exist: groups form by natural
/// batching alone (whatever queued while the previous group was committing),
/// so an uncontended request finds the committer idle and commits
/// immediately. A non-zero window trades lone-ack latency for group size and
/// is for operators who measured, not for everyone.
pub const DEFAULT_GROUP_COMMIT_WINDOW: std::time::Duration = std::time::Duration::ZERO;

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
    /// Most block records one transaction carries, and so the widest crash
    /// residue a single kill can leave (ADR 0010). Absent means
    /// [`DEFAULT_MAX_BLOCKS_PER_COMMIT`].
    ///
    /// A request larger than the cap becomes several consecutive batches, the
    /// last one closing at the ack; a request smaller than it is one batch,
    /// so a single-block PUT is unaffected whatever this says. Lower it to
    /// shorten stripe hold time and narrow the residue, raise it to amortize
    /// the journal fsync over more blocks. Not written to disk and not a
    /// format -- two processes may open one store with different caps.
    pub max_blocks_per_commit: Option<usize>,
    /// Merge the closing step of CONCURRENT requests into one transaction
    /// with one journal persist (ADR 0011). Absent means
    /// [`DEFAULT_GROUP_COMMIT`], which is `false`.
    ///
    /// Off, the write path is ADR 0010's byte for byte. On, a request's
    /// batch is handed to a per-store commit station instead of closing
    /// itself, and the committer merges whatever is queued -- up to the SAME
    /// [`StoreConfig::max_blocks_per_commit`] cap, which stays the one and
    /// only bound on transaction size, stripe hold and crash residue.
    pub group_commit: Option<bool>,
    /// Extra bounded wait the commit station spends gathering members after
    /// the FIRST one arrives, as a duration string (`"0ms"`, `"250us"`,
    /// `"2ms"`). Absent means [`DEFAULT_GROUP_COMMIT_WINDOW`], which is zero.
    ///
    /// Zero means the timer does not exist: groups are whatever natural
    /// batching delivered while the previous group committed, so a lone
    /// request adds no latency at all. Ignored entirely when
    /// [`StoreConfig::group_commit`] is off.
    pub group_commit_window: Option<String>,
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

/// Refuses a batch cap of zero.
///
/// Zero is the one unusable value: a batch that may hold no blocks commits
/// nothing and never closes, so the write path would either spin or silently
/// clamp. An operator who wants the old per-block cadence writes `1`, which
/// is a legal value and means exactly that. There is no upper bound to
/// enforce -- a cap above a request's block count simply never binds, and the
/// transaction size it implies is the operator's to weigh.
///
/// # Errors
///
/// [`ConfigError::UnsupportedMaxBlocksPerCommit`].
pub fn validate_max_blocks_per_commit(cap: usize) -> Result<(), ConfigError> {
    if cap == 0 {
        return Err(ConfigError::UnsupportedMaxBlocksPerCommit(cap));
    }
    Ok(())
}

/// Parses a `group_commit_window` string into the wait it names (ADR 0011).
///
/// humantime spelling, so `"0ms"`, `"500us"`, `"2ms"` and `"1s"` all mean
/// what they read as. A unit is REQUIRED: a bare `"0"` is refused rather than
/// guessed at, because the difference between 0 seconds and 0 milliseconds is
/// nothing but the difference between 1 and 1 is everything, and a config that
/// silently picked a unit would be a footgun the first time someone wrote
/// `group_commit_window = "1"`.
///
/// There is no upper bound to enforce. A window longer than a request's
/// patience is an operator's own mistake to make and to measure; the value
/// this function refuses is the one that cannot be a duration at all.
///
/// # Errors
///
/// [`ConfigError::UnsupportedGroupCommitWindow`] naming the offending value.
pub fn parse_group_commit_window(window: &str) -> Result<std::time::Duration, ConfigError> {
    humantime::parse_duration(window).map_err(|e| ConfigError::UnsupportedGroupCommitWindow {
        value: window.to_string(),
        reason: e.to_string(),
    })
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
    if let Some(cap) = config.store.max_blocks_per_commit {
        validate_max_blocks_per_commit(cap)?;
    }
    if let Some(window) = config.store.group_commit_window.as_deref() {
        parse_group_commit_window(window)?;
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
    /// `store.max_blocks_per_commit` is zero.
    UnsupportedMaxBlocksPerCommit(usize),
    /// `store.group_commit_window` is not a duration.
    UnsupportedGroupCommitWindow {
        /// What the file said.
        value: String,
        /// Why it is not a duration, in humantime's words.
        reason: String,
    },
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
            ConfigError::UnsupportedMaxBlocksPerCommit(cap) => write!(
                f,
                "unsupported batch cap {cap} in store.max_blocks_per_commit \
                 (expected 1 or more: a batch that may hold no blocks never \
                  closes; write 1 for one commit per block, the pre-ADR-0010 \
                  cadence)"
            ),
            ConfigError::UnsupportedGroupCommitWindow { value, reason } => write!(
                f,
                "unusable group commit window \"{value}\" in \
                 store.group_commit_window: {reason} (expected a duration \
                 with a unit, such as \"0ms\", \"250us\" or \"2ms\"; \"0ms\" \
                 is the default and means no timer at all)"
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
durability = "fsync"
inline_metadata_size = 4096
metadata_db = "fjall"
verify_on_read = true
stripe_count = 4096
max_blocks_per_commit = 32
group_commit = true
group_commit_window = "250us"

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

        assert_eq!(config.store.durability, Some(Durability::Fsync));
        assert_eq!(config.store.inline_metadata_size, Some(4096));
        assert_eq!(config.store.metadata_db, Some(StorageEngine::Fjall));
        assert_eq!(config.store.verify_on_read, Some(true));
        assert_eq!(config.store.stripe_count, Some(4096));
        assert_eq!(config.store.max_blocks_per_commit, Some(32));
        assert_eq!(config.store.group_commit, Some(true));
        assert_eq!(
            config.store.group_commit_window.as_deref(),
            Some("250us"),
            "the window is kept as written and parsed once, at resolve"
        );
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
        assert_eq!(
            config.store.max_blocks_per_commit,
            Some(DEFAULT_MAX_BLOCKS_PER_COMMIT)
        );
        assert_eq!(config.store.group_commit, Some(DEFAULT_GROUP_COMMIT));
        assert_eq!(
            config
                .store
                .group_commit_window
                .as_deref()
                .map(parse_group_commit_window)
                .transpose()
                .expect("the example's window must parse"),
            Some(DEFAULT_GROUP_COMMIT_WINDOW)
        );
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
        assert_eq!(config.store.max_blocks_per_commit, None);
        assert_eq!(config.store.group_commit, None);
        assert_eq!(config.store.group_commit_window, None);
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
            msg.contains("buffer") && msg.contains("fsync"),
            "message must list the two options: {msg}"
        );
    }

    /// The level ADR 0010 removed, with no alias. The refusal IS the
    /// migration, so the message has to say which ADR took it and which two
    /// names are left -- "unknown durability option" alone would send an
    /// operator hunting for a typo they did not make.
    #[test]
    fn removed_fdatasync_level_is_refused_with_the_two_that_remain() {
        let err = parse_str("[store]\ndurability = \"fdatasync\"\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("removed"), "message must say removed: {msg}");
        assert!(msg.contains("0010"), "message must name the ADR: {msg}");
        assert!(
            msg.contains("fsync") && msg.contains("buffer"),
            "message must name the two valid levels: {msg}"
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

    /// Zero is the one batch cap that cannot work; 1 is legal and means the
    /// pre-ADR-0010 cadence, one commit per block.
    #[test]
    fn a_zero_batch_cap_is_an_error_and_one_is_not() {
        let err = parse_str("[store]\nmax_blocks_per_commit = 0\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains('0'), "message must name the value: {msg}");
        assert!(
            msg.contains("max_blocks_per_commit"),
            "message must name the setting: {msg}"
        );

        for cap in [1, 2, DEFAULT_MAX_BLOCKS_PER_COMMIT, 4096] {
            let config = parse_str(&format!("[store]\nmax_blocks_per_commit = {cap}\n"))
                .unwrap_or_else(|e| panic!("{cap} must be accepted: {e}"));
            assert_eq!(config.store.max_blocks_per_commit, Some(cap));
        }
    }

    /// The configured default and the one the store actually uses are one
    /// value, not two that happen to match today.
    #[test]
    fn the_default_batch_cap_is_the_write_paths_own() {
        assert_eq!(
            DEFAULT_MAX_BLOCKS_PER_COMMIT,
            crate::cas::write_path::DEFAULT_MAX_BLOCKS_PER_COMMIT
        );
        validate_max_blocks_per_commit(DEFAULT_MAX_BLOCKS_PER_COMMIT)
            .expect("the built-in default must itself be a legal value");
    }

    /// The window is a duration with a unit, refused at parse time when it is
    /// not one -- next to the file that holds it, as every other store knob
    /// is.
    #[test]
    fn a_group_commit_window_that_is_not_a_duration_is_an_error() {
        let err = parse_str("[store]\ngroup_commit_window = \"soon\"\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("soon"), "message must name the value: {msg}");
        assert!(
            msg.contains("group_commit_window"),
            "message must name the setting: {msg}"
        );
        assert!(
            msg.contains("0ms"),
            "message must show a value that works: {msg}"
        );
    }

    /// A bare number is refused rather than guessed at. `"1"` could be a
    /// second or a microsecond and the difference is six orders of magnitude
    /// of ack latency, so the parser makes the operator say which.
    #[test]
    fn a_unitless_window_is_refused() {
        assert!(parse_str("[store]\ngroup_commit_window = \"1\"\n").is_err());
    }

    /// Zero is a value, not an absence: it is how the timer is turned off,
    /// and it must be spellable.
    #[test]
    fn the_window_spellings_that_must_work() {
        for (text, expected) in [
            ("0ms", std::time::Duration::ZERO),
            ("0s", std::time::Duration::ZERO),
            ("250us", std::time::Duration::from_micros(250)),
            ("2ms", std::time::Duration::from_millis(2)),
            ("1s", std::time::Duration::from_secs(1)),
        ] {
            let config = parse_str(&format!("[store]\ngroup_commit_window = \"{text}\"\n"))
                .unwrap_or_else(|e| panic!("{text} must parse: {e}"));
            assert_eq!(
                parse_group_commit_window(config.store.group_commit_window.as_deref().unwrap())
                    .unwrap(),
                expected,
                "{text}"
            );
        }
    }

    /// Group commit is off unless the file says otherwise, and `false` is
    /// distinguishable from absent -- an operator who writes it out
    /// explicitly gets the same behaviour, not a different code path.
    #[test]
    fn group_commit_is_off_by_default_and_false_is_a_value() {
        // The ADR 0011 default is off, with no timer.
        const { assert!(!DEFAULT_GROUP_COMMIT) };
        assert_eq!(DEFAULT_GROUP_COMMIT_WINDOW, std::time::Duration::ZERO);

        let config = parse_str("[store]\ngroup_commit = false\n").unwrap();
        assert_eq!(config.store.group_commit, Some(false));
        let config = parse_str("[store]\n").unwrap();
        assert_eq!(config.store.group_commit, None);
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
        for durability in [Durability::Buffer, Durability::Fsync] {
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
