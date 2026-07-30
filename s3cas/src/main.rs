use std::path::PathBuf;

use anyhow::Result;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use http_body_util::Full;
use prometheus::Encoder;
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

use s3cas::cas::{CasFS, StorageEngine};
use s3cas::check::{CheckConfig, check_integrity};
use s3cas::inspect::{disk_space, num_keys};
use s3cas::metastore::Durability;
use s3cas::retrieve::{RetrieveConfig, retrieve};

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Parser, Debug)]
pub struct ServerConfig {
    #[arg(long, default_value = ".")]
    fs_root: PathBuf,

    #[arg(long, default_value = ".")]
    meta_root: PathBuf,

    #[arg(long, default_value = "localhost")]
    host: String,

    #[arg(long, default_value = "8014")]
    port: u16,

    #[arg(long, default_value = "localhost")]
    metric_host: String,

    #[arg(long, default_value = "9100")]
    metric_port: u16,

    #[arg(long, help = "leave empty to disable it")]
    inline_metadata_size: Option<usize>,

    #[arg(long, required = true, display_order = 1000)]
    access_key: Option<String>,

    #[arg(long, required = true, display_order = 1000)]
    secret_key: Option<String>,

    #[arg(
        long,
        default_value = "fjall",
        help = "Metadata DB  (fjall, fjall_notx)"
    )]
    metadata_db: StorageEngine,

    #[arg(
        long,
        default_value = "fsync",
        help = "Durability level (buffer, fsync, fdatasync); fsync is strongest"
    )]
    durability: Durability,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Inspect DB
    Inspect {
        #[arg(long, default_value = ".")]
        meta_root: PathBuf,

        #[arg(
            long,
            default_value = "fjall",
            help = "Metadata DB  (fjall, fjall_notx)"
        )]
        metadata_db: StorageEngine,

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
            command,
            meta_root,
            metadata_db,
        } => match command {
            InspectCommand::NumKeys { bucket_name } => {
                let num_keys = num_keys(meta_root, metadata_db, &bucket_name)?;
                println!("Number of keys in bucket '{}': {}", bucket_name, num_keys);
            }
            InspectCommand::DiskSpace => {
                let disk_space = disk_space(meta_root, metadata_db)?;
                println!("Disk space: {}", disk_space);
            }
        },
        Command::Retrieve(config) => retrieve(config)?,
        Command::Check(config) => check_integrity(config)?,
        Command::Server(config) => {
            run(config)?;
        }
    }
    Ok(())
}

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::service::S3ServiceBuilder;

#[tokio::main]
async fn run(args: ServerConfig) -> anyhow::Result<()> {
    let storage_engine = args.metadata_db;

    // provider
    let metrics = s3cas::metrics::SharedMetrics::new();
    let casfs = CasFS::single_namespace(
        args.fs_root.clone(),
        args.meta_root.clone(),
        metrics.to_cas(),
        storage_engine,
        args.inline_metadata_size,
        Some(args.durability),
        None,
        false, // verify_on_read: off until the config file can turn it on
    )?;
    let s3fs = s3cas::s3fs::S3FS::new(casfs, metrics.clone());
    let s3fs = s3cas::metrics::MetricFs::new(s3fs, metrics.clone());

    // Setup S3 service
    let service = {
        let mut b = S3ServiceBuilder::new(s3fs);

        // Enable authentication
        if let (Some(ak), Some(sk)) = (args.access_key, args.secret_key) {
            b.set_auth(s3s::auth::SimpleAuth::from_single(ak, sk));
            info!("authentication is enabled");
        }

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
