use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use tracing::info;

mod cmd;
mod conn;
mod namespace;
mod property;
mod resp;
mod server;
mod storage;

#[derive(Parser, Debug)]
#[clap(name = "respd", about = "Redis-compatible server using metastore")]
struct Opt {
    /// Path to the data directory
    #[clap(long, default_value = "./data")]
    data_dir: PathBuf,

    /// Port to listen on
    #[clap(long, default_value = "6379")]
    port: u16,

    /// Host to bind to
    #[clap(long, default_value = "127.0.0.1")]
    host: String,

    /// Admin password for authentication
    /// If not provided, all connections are automatically granted admin privileges
    #[clap(long)]
    admin: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let opt = Opt::parse();

    info!("Data directory: {:?}", opt.data_dir);

    // Create data directory if it doesn't exist
    if !opt.data_dir.exists() {
        std::fs::create_dir_all(&opt.data_dir)?;
    }

    // Initialize storage
    // set the inlined metadata size to 1byte effectily enabling it for all keys
    let storage = storage::Storage::new(opt.data_dir.clone(), Some(1))?;

    // Start server
    info!("Starting respd server on {}:{}", opt.host, opt.port);
    if opt.admin.is_some() {
        info!("Admin authentication is required");
    } else {
        info!("Admin authentication is disabled - all connections have admin privileges");
    }
    let addr = format!("{}:{}", opt.host, opt.port);
    server::run(addr, storage, opt.admin).await
}
