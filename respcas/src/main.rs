use anyhow::Result;
use clap::Parser;
use std::path::{Path, PathBuf};
use tracing::info;

use cas_storage::StoreOptions;
use cas_storage::config::{
    self, DEFAULT_RESP_DATA_DIR, DEFAULT_RESP_HOST, DEFAULT_RESP_INLINE_METADATA_SIZE,
    DEFAULT_RESP_MAX_VALUE_SIZE, DEFAULT_RESP_PORT, QssStorageConfig,
};

mod cmd;
mod conn;
mod content;
mod namespace;
mod property;
mod resp;
mod server;
mod storage;

/// respcas's flags.
///
/// Everything the config file can also supply is an `Option` without a
/// `default_value`: clap cannot tell a flag the operator passed from one it
/// defaulted, so a `default_value` here would silently outrank the config
/// file. The defaults live in `cas_storage::config` and are applied by
/// [`resolve`].
#[derive(Parser, Debug, Default)]
#[clap(name = "respcas", about = "Redis-compatible server using metastore")]
struct Opt {
    /// Path to qss_storage.toml
    /// (default: ./qss_storage.toml, then /etc/qss_storage/qss_storage.toml)
    #[clap(long)]
    config: Option<PathBuf>,

    /// Path to the data directory (default ./data)
    #[clap(long)]
    data_dir: Option<PathBuf>,

    /// Port to listen on (default 6379)
    #[clap(long)]
    port: Option<u16>,

    /// Host to bind to (default 127.0.0.1)
    #[clap(long)]
    host: Option<String>,

    /// Admin password for authentication
    /// If not provided, all connections are automatically granted admin privileges
    #[clap(long)]
    admin: Option<String>,
}

/// [`Opt`] merged with the config file and the built-in defaults.
#[derive(Debug, Clone, PartialEq)]
struct ResolvedConfig {
    data_dir: PathBuf,
    host: String,
    port: u16,
    admin: Option<String>,
    /// Largest value a client may send (ADR 0014). Ingest is
    /// buffer-then-write, so this is the memory one writing connection
    /// costs.
    max_value_size: usize,
    /// How the store is opened. The same `[store]` table s3cas and fsck read,
    /// because since ADR 0014 respcas opens the same kind of store they do.
    store: StoreOptions,
}

/// Merges the flags over the config file over the built-in defaults.
///
/// The `[store]` table resolves exactly as it does for every other binary
/// here, with one respcas-specific default on top: the inline threshold. A
/// value at or below it stays in its own record, which is how respcas has
/// always stored everything, and the built-in 1 byte keeps that true for a
/// deployment that configures nothing.
///
/// # Errors
///
/// [`config::ConfigError`] if the `[store]` table does not resolve -- a hash
/// this build does not have, an unusable stripe count or batch cap, or a
/// group-commit window that will not parse.
fn resolve(flags: Opt, config: &QssStorageConfig) -> Result<ResolvedConfig, config::ConfigError> {
    let resp = config.resp.clone().unwrap_or_default();
    let store = StoreOptions::resolve(None, None, None, None, &config.store)?;

    Ok(ResolvedConfig {
        data_dir: flags
            .data_dir
            .or(resp.data_dir)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_RESP_DATA_DIR)),
        host: flags
            .host
            .or(resp.host)
            .unwrap_or_else(|| DEFAULT_RESP_HOST.to_string()),
        port: flags.port.or(resp.port).unwrap_or(DEFAULT_RESP_PORT),
        admin: flags.admin.or(resp.admin_password),
        max_value_size: resp.max_value_size.unwrap_or(DEFAULT_RESP_MAX_VALUE_SIZE),
        store: StoreOptions {
            inline_metadata_size: Some(
                store
                    .inline_metadata_size
                    .unwrap_or(DEFAULT_RESP_INLINE_METADATA_SIZE),
            ),
            ..store
        },
    })
}

/// Loads the config file and says where it came from, so an operator can tell
/// from the log which file (if any) the process is actually running on.
fn load_config(explicit: Option<&Path>) -> Result<QssStorageConfig> {
    let (config, source) = config::load(explicit)?;
    match source {
        Some(path) => info!("configuration loaded from {}", path.display()),
        None => info!("no configuration file found, using built-in defaults"),
    }
    Ok(config)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let opt = Opt::parse();
    let file = load_config(opt.config.as_deref())?;
    let cfg = resolve(opt, &file)?;

    info!("Data directory: {:?}", cfg.data_dir);

    // Create data directory if it doesn't exist
    if !cfg.data_dir.exists() {
        std::fs::create_dir_all(&cfg.data_dir)?;
    }

    // Initialize storage. The inline threshold defaults to 1 byte, which
    // effectively inlines every value; the hash matters only for a store being
    // created now, since an existing one is opened on the header it already
    // has.
    let storage = storage::Storage::new(cfg.data_dir.clone(), cfg.store)?;

    // Start server
    info!("Starting respcas server on {}:{}", cfg.host, cfg.port);
    if cfg.admin.is_some() {
        info!("Admin authentication is required");
    } else {
        info!("Admin authentication is disabled - all connections have admin privileges");
    }
    let addr = format!("{}:{}", cfg.host, cfg.port);
    server::run(addr, storage, cfg.admin, cfg.max_value_size).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use cas_storage::{Durability, Hasher};

    fn config(text: &str) -> QssStorageConfig {
        config::parse(text, Path::new("test.toml")).expect("test config must parse")
    }

    #[test]
    fn defaults_apply_when_neither_flags_nor_file_say_anything() {
        let cfg = resolve(Opt::default(), &QssStorageConfig::default()).unwrap();

        assert_eq!(cfg.data_dir, PathBuf::from(DEFAULT_RESP_DATA_DIR));
        assert_eq!(cfg.host, DEFAULT_RESP_HOST);
        assert_eq!(cfg.port, DEFAULT_RESP_PORT);
        assert_eq!(cfg.admin, None);
        assert_eq!(cfg.store.inline_metadata_size, Some(1));
        assert_eq!(cfg.store.durability, Durability::Fsync);
        assert_eq!(cfg.store.hasher, Hasher::Blake3W32);
    }

    #[test]
    fn the_config_file_beats_the_defaults() {
        let file = config(
            "[store]\ndurability = \"buffer\"\ninline_metadata_size = 64\n\n\
             [store.hash]\nwidth = 16\n\n\
             [resp]\nhost = \"0.0.0.0\"\nport = 6380\n\
             data_dir = \"/var/lib/respcas\"\nadmin_password = \"hunter2\"\n",
        );
        let cfg = resolve(Opt::default(), &file).unwrap();

        assert_eq!(cfg.data_dir, PathBuf::from("/var/lib/respcas"));
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.port, 6380);
        assert_eq!(cfg.admin.as_deref(), Some("hunter2"));
        assert_eq!(cfg.store.inline_metadata_size, Some(64));
        assert_eq!(cfg.store.durability, Durability::Buffer);
        assert_eq!(cfg.store.hasher, Hasher::Blake3W16);
    }

    #[test]
    fn a_flag_beats_the_config_file() {
        let file = config(
            "[resp]\nhost = \"0.0.0.0\"\nport = 6380\n\
             data_dir = \"/var/lib/respcas\"\nadmin_password = \"hunter2\"\n",
        );
        let flags = Opt {
            port: Some(7000),
            host: Some("127.0.0.2".to_string()),
            data_dir: Some(PathBuf::from("/tmp/respcas")),
            admin: Some("flagpass".to_string()),
            ..Opt::default()
        };
        let cfg = resolve(flags, &file).unwrap();

        assert_eq!(cfg.port, 7000);
        assert_eq!(cfg.host, "127.0.0.2");
        assert_eq!(cfg.data_dir, PathBuf::from("/tmp/respcas"));
        assert_eq!(cfg.admin.as_deref(), Some("flagpass"));
    }
}
