use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Result, bail};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use http_body_util::Full;
use prometheus::Encoder;
use tracing::{Level, info, warn};
use tracing_subscriber::FmtSubscriber;

use cas_storage::StoreOptions;
use cas_storage::config::{
    self, DEFAULT_METRICS_HOST, DEFAULT_METRICS_PORT, DEFAULT_MULTIPART_STALE_TTL_DAYS,
    DEFAULT_S3_HOST, DEFAULT_S3_PORT, QssStorageConfig,
};
use s3cas::cas::{CasFS, StorageEngine};
use s3cas::check::{CheckConfig, check_integrity};
use s3cas::inspect::{disk_space, headers, num_keys};
use s3cas::metastore::Durability;
use s3cas::retrieve::{RetrieveConfig, retrieve};

/// Help text for every `--config` flag in this binary.
const CONFIG_HELP: &str = "Path to qss_storage.toml (default: ./qss_storage.toml, then \
                           /etc/qss_storage/qss_storage.toml)";

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// The `server` subcommand's flags.
///
/// Every field that the config file can also supply is an `Option` without a
/// `default_value`: clap cannot distinguish a flag the operator passed from
/// one it defaulted, so a `default_value` here would silently outrank the
/// config file. The defaults live in `cas_storage::config` and are applied by
/// [`resolve_server`].
///
/// `fs_root` and `meta_root` keep their clap defaults: the config schema has
/// no key for them, so there is nothing for a flag to outrank.
#[derive(Parser, Debug, Default)]
pub struct ServerConfig {
    #[arg(long, help = CONFIG_HELP)]
    config: Option<PathBuf>,

    #[arg(long, default_value = ".")]
    fs_root: PathBuf,

    #[arg(long, default_value = ".")]
    meta_root: PathBuf,

    #[arg(long, help = "Address to bind (default localhost)")]
    host: Option<String>,

    #[arg(long, help = "Port to bind (default 8014)")]
    port: Option<u16>,

    #[arg(long, help = "Address for the metrics endpoint (default localhost)")]
    metric_host: Option<String>,

    #[arg(long, help = "Port for the metrics endpoint (default 9100)")]
    metric_port: Option<u16>,

    #[arg(long, help = "leave empty to disable it")]
    inline_metadata_size: Option<usize>,

    #[arg(long, display_order = 1000)]
    access_key: Option<String>,

    #[arg(long, display_order = 1000)]
    secret_key: Option<String>,

    #[arg(long, help = "Metadata DB (fjall); default fjall")]
    metadata_db: Option<StorageEngine>,

    #[arg(
        long,
        help = "Durability level (buffer, fsync, fdatasync); default fsync, which is strongest"
    )]
    durability: Option<Durability>,

    #[arg(
        long,
        help = "Days before an unfinished multipart upload is aborted by the GC; 0 disables it (default 7)"
    )]
    multipart_stale_ttl_days: Option<u64>,
}

/// A [`ServerConfig`] merged with the config file and the built-in defaults:
/// every field decided, nothing left to resolve.
#[derive(Debug)]
struct ResolvedServerConfig {
    fs_root: PathBuf,
    meta_root: PathBuf,
    host: String,
    port: u16,
    metric_host: String,
    metric_port: u16,
    access_key: String,
    secret_key: String,
    store: StoreOptions,
    /// Age at which the GC aborts an unfinished multipart upload, in days.
    /// Zero means the sweep is not spawned at all (ADR 0003).
    multipart_stale_ttl_days: u64,
}

/// Merges the server flags over the config file over the built-in defaults.
///
/// # Errors
///
/// If the S3 credentials are not complete. Authentication is mandatory for the
/// server (it was `required = true` on the flags before the config file
/// existed), so a half-configured pair is a refusal to start rather than an
/// unauthenticated endpoint.
fn resolve_server(flags: ServerConfig, config: &QssStorageConfig) -> Result<ResolvedServerConfig> {
    let s3 = config.s3.clone().unwrap_or_default();
    let metrics = s3.metrics.clone().unwrap_or_default();
    let multipart = config.multipart.clone().unwrap_or_default();

    let store = StoreOptions::resolve(
        flags.metadata_db,
        flags.durability,
        flags.inline_metadata_size,
        &config.store,
    )?;

    let (Some(access_key), Some(secret_key)) = (
        flags.access_key.or(s3.access_key),
        flags.secret_key.or(s3.secret_key),
    ) else {
        bail!(
            "S3 credentials are required: pass --access-key and --secret-key, \
             or set s3.access_key and s3.secret_key in the config file"
        );
    };

    Ok(ResolvedServerConfig {
        fs_root: flags.fs_root,
        meta_root: flags.meta_root,
        host: flags
            .host
            .or(s3.host)
            .unwrap_or_else(|| DEFAULT_S3_HOST.to_string()),
        port: flags.port.or(s3.port).unwrap_or(DEFAULT_S3_PORT),
        metric_host: flags
            .metric_host
            .or(metrics.host)
            .unwrap_or_else(|| DEFAULT_METRICS_HOST.to_string()),
        metric_port: flags
            .metric_port
            .or(metrics.port)
            .unwrap_or(DEFAULT_METRICS_PORT),
        access_key,
        secret_key,
        store,
        multipart_stale_ttl_days: flags
            .multipart_stale_ttl_days
            .or(multipart.stale_ttl_days)
            .unwrap_or(DEFAULT_MULTIPART_STALE_TTL_DAYS),
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

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Inspect DB
    Inspect {
        #[arg(long, help = CONFIG_HELP)]
        config: Option<PathBuf>,

        #[arg(long, default_value = ".")]
        meta_root: PathBuf,

        #[arg(long, help = "Metadata DB (fjall); default fjall")]
        metadata_db: Option<StorageEngine>,

        #[command(subcommand)]
        command: InspectCommand,
    },

    /// retrieve an object
    Retrieve(RetrieveConfig),

    /// Check object integrity
    Check(CheckConfig),

    /// Start S3-cas server
    Server(ServerConfig),
}

#[derive(Debug, Subcommand)]
pub enum InspectCommand {
    // number of keys
    NumKeys {
        /// Name of the bucket to count keys for
        bucket_name: String,
    },
    DiskSpace,

    /// Print the QSST store header of every metadata DB under the meta root
    Header,
}

fn setup_tracing() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

fn main() -> Result<()> {
    dotenv::dotenv().ok();

    setup_tracing();
    let cli = Cli::parse();
    match cli.command {
        Command::Inspect {
            config,
            command,
            meta_root,
            metadata_db,
        } => {
            let file = load_config(config.as_deref())?;
            let store = StoreOptions::resolve(metadata_db, None, None, &file.store)?;
            match command {
                InspectCommand::NumKeys { bucket_name } => {
                    let num_keys = num_keys(meta_root, &store, &bucket_name)?;
                    println!("Number of keys in bucket '{}': {}", bucket_name, num_keys);
                }
                InspectCommand::DiskSpace => {
                    // Two databases, two numbers: reporting either one alone
                    // reads as the store's footprint while being a fraction
                    // of it.
                    let space = disk_space(meta_root, &store)?;
                    println!("Disk space, namespace DB: {}", space.namespace);
                    match space.blocks {
                        Some(blocks) => println!("Disk space, blocks DB:    {blocks}"),
                        None => println!("Disk space, blocks DB:    (none)"),
                    }
                    println!("Disk space, total:        {}", space.total());
                }
                InspectCommand::Header => {
                    for entry in headers(meta_root, &store)? {
                        print!("{}", entry.render());
                    }
                }
            }
        }
        Command::Retrieve(args) => {
            let file = load_config(args.config.as_deref())?;
            let store = StoreOptions::resolve(args.metadata_db, None, None, &file.store)?;
            retrieve(args, store)?
        }
        Command::Check(args) => {
            let file = load_config(args.config.as_deref())?;
            let store = StoreOptions::resolve(args.metadata_db, None, None, &file.store)?;
            check_integrity(args, store)?
        }
        Command::Server(flags) => {
            let file = load_config(flags.config.as_deref())?;
            let resolved = resolve_server(flags, &file)?;
            run(resolved)?;
        }
    }
    Ok(())
}

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::service::S3ServiceBuilder;

/// Seconds in a day: the TTL is configured in days and consumed as a
/// [`Duration`].
const SECONDS_PER_DAY: u64 = 24 * 60 * 60;

/// The shortest sweep interval, and the floor under `ttl/20`: a one-hour
/// period keeps a short TTL from turning the GC into a busy loop over the
/// parts tree (ADR 0003 decision 8 fixes both numbers).
const MIN_SWEEP_PERIOD: Duration = Duration::from_secs(60 * 60);

/// Spawns the stale-upload sweeper (ADR 0003 decision 8), or nothing when the
/// TTL is zero.
///
/// `casfs` is a clone of the one the S3 service holds: same namespace DB, same
/// `SharedBlockStore`, so the sweep's decrements serialize against live
/// traffic on the same stripes (ADR 0006). `shutdown` fires where the accept
/// loop breaks on ctrl-c, so the task ends with the server rather than being
/// dropped mid-sweep.
fn spawn_stale_upload_gc(
    casfs: CasFS,
    metrics: s3cas::metrics::SharedMetrics,
    ttl_days: u64,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Option<tokio::task::JoinHandle<()>> {
    if ttl_days == 0 {
        info!("stale multipart upload GC disabled (multipart.stale_ttl_days = 0)");
        return None;
    }

    let ttl = Duration::from_secs(ttl_days.saturating_mul(SECONDS_PER_DAY));
    let period = std::cmp::max(ttl / 20, MIN_SWEEP_PERIOD);
    info!(
        "stale multipart upload GC: TTL {ttl_days} days, sweeping every {} minutes",
        period.as_secs() / 60
    );

    Some(tokio::spawn(async move {
        // The first tick lands one full period from now, not at startup. A
        // store that has just booted has nothing that became stale while it
        // was down that will not still be stale in an hour, operators expect
        // a quiet startup (a sweep is disk work, and boot is when a store is
        // busiest), and waiting costs nothing: the residue this collects has
        // already been sitting there for days.
        let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        // A sweep that overruns its period must not then fire back to back.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let stats = s3cas::cas::sweep_stale_uploads(&casfs, ttl).await;
                    metrics.uploads_reaped(stats.uploads_reaped);
                    metrics.orphan_parts_reaped(stats.orphan_parts_reaped);
                    if stats != Default::default() {
                        info!(
                            "stale-upload sweep: {} uploads and {} orphan parts reaped, {} errors",
                            stats.uploads_reaped, stats.orphan_parts_reaped, stats.errors
                        );
                    }
                }
                _ = shutdown.changed() => break,
            }
        }
        tracing::debug!("stale multipart upload GC stopped");
    }))
}

#[tokio::main]
async fn run(args: ResolvedServerConfig) -> anyhow::Result<()> {
    // provider
    let metrics = s3cas::metrics::SharedMetrics::new();
    let casfs = CasFS::single_namespace(
        args.fs_root.clone(),
        args.meta_root.clone(),
        metrics.to_cas(),
        args.store.metadata_db,
        args.store.inline_metadata_size,
        Some(args.store.durability),
        Some(args.store.header_spec()),
        args.store.verify_on_read,
    )?;

    // store.hash applies at creation only: an existing store is addressed by
    // the hash in its (immutable) header. Say so rather than let an operator
    // believe a width change took effect.
    let opened = casfs.hasher();
    if opened != args.store.hasher {
        warn!(
            "configured block hash {:?} ignored: this store's header says {:?}, \
             and the header wins (create a new store to change it)",
            args.store.hasher, opened
        );
    }
    info!(
        "store: hash {:?}, {} durability, metadata db {}, verify_on_read {}",
        opened, args.store.durability, args.store.metadata_db, args.store.verify_on_read
    );

    // The GC's handle, taken before the service consumes the original. A
    // clone shares the SharedBlockStore Arc rather than opening the store
    // twice, which is what ADR 0006's one-store-one-instance rule requires.
    let gc_casfs = casfs.clone();

    let s3fs = s3cas::s3fs::S3FS::new(casfs, metrics.clone());
    let s3fs = s3cas::metrics::MetricFs::new(s3fs, metrics.clone());

    let (gc_shutdown, gc_shutdown_rx) = tokio::sync::watch::channel(false);
    let gc_task = spawn_stale_upload_gc(
        gc_casfs,
        metrics.clone(),
        args.multipart_stale_ttl_days,
        gc_shutdown_rx,
    );

    // Setup S3 service
    let service = {
        let mut b = S3ServiceBuilder::new(s3fs);

        // Authentication is mandatory; resolve_server refused to start
        // without a complete credential pair.
        b.set_auth(s3s::auth::SimpleAuth::from_single(
            args.access_key,
            args.secret_key,
        ));
        info!("authentication is enabled");

        b.build()
    };

    // Run server
    // S3 listener
    let listener = tokio::net::TcpListener::bind((args.host.as_str(), args.port)).await?;
    let local_addr = listener.local_addr()?;

    let hyper_service = service;

    // metrics server
    // Add after the main listener setup
    let metrics_listener =
        tokio::net::TcpListener::bind((args.metric_host.as_str(), args.metric_port)).await?;
    let metrics_addr = metrics_listener.local_addr()?;

    info!("metrics server is running at http://{metrics_addr}");

    let metrics_service = hyper::service::service_fn(
        move |req: hyper::Request<hyper::body::Incoming>| async move {
            match (req.method(), req.uri().path()) {
                (&hyper::Method::GET, "/metrics") => {
                    let mut buffer = Vec::new();
                    let encoder = prometheus::TextEncoder::new();
                    let metric_families = prometheus::gather();
                    encoder.encode(&metric_families, &mut buffer).unwrap();

                    Ok::<_, std::convert::Infallible>(
                        hyper::Response::builder()
                            .status(200)
                            .header(hyper::header::CONTENT_TYPE, "text/plain; version=0.0.4")
                            .body(Full::new(Bytes::from(buffer)))
                            .unwrap(),
                    )
                }
                _ => Ok::<_, std::convert::Infallible>(
                    hyper::Response::builder()
                        .status(404)
                        .body(Full::new(Bytes::from("Not Found")))
                        .unwrap(),
                ),
            }
        },
    );

    let http_server = ConnBuilder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();

    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

    info!("server is running at http://{local_addr}");

    loop {
        tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok((socket,_)) => {
                        let conn = http_server.serve_connection(TokioIo::new(socket), hyper_service.clone());
                        let conn = graceful.watch(conn.into_owned());
                        tokio::spawn(async move {
                            let _ = conn.await;
                        });
                        continue;
                    }
                    Err(err) => {
                        tracing::error!("error accepting connection: {err}");
                        continue;
                    }
                }
            }
            res = metrics_listener.accept() => {
                match res {
                    Ok((socket, _)) =>{
                        let conn = http_server.serve_connection(TokioIo::new(socket), metrics_service);
                        let conn = graceful.watch(conn.into_owned());
                        tokio::spawn(async move {
                            let _ = conn.await;
                        });
                        continue;

                    }// (socket, metrics_service.clone()),
                    Err(err) => {
                        tracing::error!("error accepting metrics connection: {err}");
                        continue;
                    }
                }
            }
            _ = ctrl_c.as_mut() => {
                break;
            }
        };
    }

    // The same signal that ends the accept loop ends the sweeper.
    let _ = gc_shutdown.send(true);
    if let Some(task) = gc_task {
        tokio::select! {
            _ = task => tracing::debug!("stale-upload GC stopped"),
            () = tokio::time::sleep(Duration::from_secs(5)) => {
                // A sweep in flight only reaches the signal between records,
                // and a large store's sweep can outlast any shutdown budget.
                // Leaving it is safe: each step is crash-safe on its own
                // (record first, blocks second), so an abandoned sweep is
                // exactly the crash case that ordering was chosen for.
                tracing::debug!("stale-upload GC still sweeping; not waiting for it");
            }
        }
    }

    tokio::select! {
        () = graceful.shutdown() => {
             tracing::debug!("Gracefully shutdown!");
        },
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
             tracing::debug!("Waited 10 seconds for graceful shutdown, aborting...");
        }
    }

    info!("server is stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cas_storage::Hasher;

    /// Flags with nothing set except the credentials, which the server refuses
    /// to start without.
    fn flags_with_credentials() -> ServerConfig {
        ServerConfig {
            access_key: Some("flag-ak".to_string()),
            secret_key: Some("flag-sk".to_string()),
            ..ServerConfig::default()
        }
    }

    fn config(text: &str) -> QssStorageConfig {
        config::parse(text, Path::new("test.toml")).expect("test config must parse")
    }

    #[test]
    fn defaults_apply_when_neither_flags_nor_file_say_anything() {
        let resolved =
            resolve_server(flags_with_credentials(), &QssStorageConfig::default()).unwrap();

        assert_eq!(resolved.host, DEFAULT_S3_HOST);
        assert_eq!(resolved.port, DEFAULT_S3_PORT);
        assert_eq!(resolved.metric_host, DEFAULT_METRICS_HOST);
        assert_eq!(resolved.metric_port, DEFAULT_METRICS_PORT);
        assert_eq!(resolved.store.durability, Durability::Fsync);
        assert_eq!(resolved.store.metadata_db, StorageEngine::Fjall);
        assert_eq!(resolved.store.hasher, Hasher::Blake3W32);
        assert!(!resolved.store.verify_on_read);
        assert_eq!(
            resolved.multipart_stale_ttl_days,
            DEFAULT_MULTIPART_STALE_TTL_DAYS
        );
    }

    /// The GC's TTL follows the same flag-over-file-over-default rule as every
    /// other setting, and 0 is a value like any other -- it is how the sweep
    /// is turned off, so it must survive the merge instead of reading as
    /// "unset" and collecting the 7 day default.
    #[test]
    fn the_multipart_ttl_merges_like_every_other_setting() {
        let file = config("[multipart]\nstale_ttl_days = 30\n");
        assert_eq!(
            resolve_server(flags_with_credentials(), &file)
                .unwrap()
                .multipart_stale_ttl_days,
            30
        );

        let flags = ServerConfig {
            multipart_stale_ttl_days: Some(3),
            ..flags_with_credentials()
        };
        assert_eq!(
            resolve_server(flags, &file)
                .unwrap()
                .multipart_stale_ttl_days,
            3
        );

        let flags = ServerConfig {
            multipart_stale_ttl_days: Some(0),
            ..flags_with_credentials()
        };
        assert_eq!(
            resolve_server(flags, &file)
                .unwrap()
                .multipart_stale_ttl_days,
            0,
            "0 disables the sweep; it must not fall through to the default"
        );
    }

    #[test]
    fn the_config_file_beats_the_defaults() {
        let file = config(
            "[store]\ndurability = \"buffer\"\n\n\
             [store.hash]\nwidth = 16\n\n\
             [s3]\nhost = \"0.0.0.0\"\nport = 9000\n\n\
             [s3.metrics]\nport = 9101\n",
        );
        let resolved = resolve_server(flags_with_credentials(), &file).unwrap();

        assert_eq!(resolved.host, "0.0.0.0");
        assert_eq!(resolved.port, 9000);
        assert_eq!(resolved.metric_port, 9101);
        // Not in the file: still the default.
        assert_eq!(resolved.metric_host, DEFAULT_METRICS_HOST);
        assert_eq!(resolved.store.durability, Durability::Buffer);
        assert_eq!(resolved.store.hasher, Hasher::Blake3W16);
    }

    #[test]
    fn a_flag_beats_the_config_file() {
        let file = config(
            "[store]\ndurability = \"buffer\"\n\n\
             [s3]\nport = 9000\naccess_key = \"file-ak\"\nsecret_key = \"file-sk\"\n",
        );
        let flags = ServerConfig {
            port: Some(7000),
            durability: Some(Durability::Fdatasync),
            ..flags_with_credentials()
        };
        let resolved = resolve_server(flags, &file).unwrap();

        assert_eq!(resolved.port, 7000);
        assert_eq!(resolved.store.durability, Durability::Fdatasync);
        assert_eq!(resolved.access_key, "flag-ak");
        assert_eq!(resolved.secret_key, "flag-sk");
    }

    #[test]
    fn credentials_may_come_from_the_config_file_alone() {
        let file = config("[s3]\naccess_key = \"file-ak\"\nsecret_key = \"file-sk\"\n");
        let resolved = resolve_server(ServerConfig::default(), &file).unwrap();

        assert_eq!(resolved.access_key, "file-ak");
        assert_eq!(resolved.secret_key, "file-sk");
    }

    #[test]
    fn incomplete_credentials_refuse_to_start() {
        let err =
            resolve_server(ServerConfig::default(), &QssStorageConfig::default()).unwrap_err();
        assert!(
            err.to_string().contains("--access-key"),
            "message must say how to fix it: {err}"
        );

        // Half a pair is no better than none: never start unauthenticated.
        let file = config("[s3]\naccess_key = \"file-ak\"\n");
        assert!(resolve_server(ServerConfig::default(), &file).is_err());
    }
}
