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

/// Address respcas binds by default.
pub const DEFAULT_RESP_HOST: &str = "127.0.0.1";

/// Port respcas binds by default (the Redis port).
pub const DEFAULT_RESP_PORT: u16 = 6379;

/// Data directory respcas uses by default.
pub const DEFAULT_RESP_DATA_DIR: &str = "./data";

/// respcas inlines every value it can: a 1 byte threshold means "inline
/// everything that fits", which is how respcas has always run.
pub const DEFAULT_RESP_INLINE_METADATA_SIZE: usize = 1;

/// Largest value respcas accepts by default: 64 MiB (ADR 0014).
///
/// zdb caps its payload at 8 MiB; respcas serves broader workloads, and the
/// cost of the larger cap is memory per concurrent writer, not per stored
/// object. A client with objects bigger than this belongs on the S3 face,
/// which has multipart.
pub const DEFAULT_RESP_MAX_VALUE_SIZE: usize = 64 * 1024 * 1024;

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
    /// respcas-only settings.
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
/// Consumed by s3cas only; respcas exports no metrics.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    /// Address the metrics endpoint binds.
    pub host: Option<String>,
    /// Port the metrics endpoint binds.
    pub port: Option<u16>,
}

/// The `[resp]` table: respcas server settings.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RespConfig {
    /// Address respcas binds.
    pub host: Option<String>,
    /// Port respcas binds.
    pub port: Option<u16>,
    /// Directory holding respcas's database.
    pub data_dir: Option<PathBuf>,
    /// Password required for admin commands. Absent means every connection is
    /// granted admin privileges.
    pub admin_password: Option<String>,
    /// Largest value a client may send, in bytes (ADR 0014).
    ///
    /// RESP has no chunked framing, so ingest is buffer-then-write: a value
    /// is whole in memory before the write path sees it. This bounds that
    /// honestly rather than pretending the daemon streams -- the high-water
    /// mark is this times the connections writing at once. A command
    /// declaring a longer value is refused before its bytes are read.
    pub max_value_size: Option<usize>,
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

mod error;
#[cfg(test)]
mod tests;

pub use error::ConfigError;
