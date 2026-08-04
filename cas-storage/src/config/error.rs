use std::fmt::{self, Display, Formatter};
use std::io;
use std::path::PathBuf;

use super::{DEFAULT_HASH_ALGO, MAX_STRIPE_COUNT};

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
