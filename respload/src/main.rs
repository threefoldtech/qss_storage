//! `respload`: a load generator and binary-safe client for RESP2 servers.
//!
//! It talks to respcas, and it talks to anything else that speaks RESP2 --
//! hero_db on `127.0.0.1:6378` or its `resp.sock`, or a stock redis for a
//! baseline. That portability is the point: a throughput number is only worth
//! having next to another one taken the same way, by the same driver, over the
//! same wire.
//!
//! It is a driver, not a judge. Every sub-command reports what happened and
//! exits; nothing in here decides whether a number is good. The campaign's
//! phase scripts own that, and this tool stays usable by hand because of it.
//!
//! Two sub-commands:
//!
//!   cmd    one command, reply rendered as text, raw bytes, hex or a length
//!   bench  a fixed-duration load, one TSV row on stdout
//!
//! Argument encodings, so a shell can express bytes it cannot hold:
//!
//!   plain        utf-8 as written
//!   hex:a1b2..   raw bytes from hex
//!   @path        the file's bytes
//!   rand:N       N pseudorandom bytes
//!
//! Against respcas, `--select` names a namespace and `--password` unlocks it.
//! Against hero_db, `--select` is the numeric database index; the wire shape is
//! identical and the server decides what the argument means.

mod bench;
mod resp;
mod rng;

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::resp::{Conn, Reply, Target};
use crate::rng::{SplitMix64, seed_from_clock};

#[derive(Parser)]
#[command(
    name = "respload",
    version,
    about = "RESP2 load generator and binary-safe client"
)]
struct Cli {
    #[arg(short = 'H', long, default_value = "127.0.0.1", global = true)]
    host: String,
    #[arg(short, long, default_value_t = 16379, global = true)]
    port: u16,
    /// Unix socket path instead of TCP (hero_db's resp.sock).
    #[arg(long, global = true)]
    unix: Option<PathBuf>,
    /// Namespace (respcas) or database index (hero_db) to SELECT first.
    #[arg(short, long, default_value = "", global = true)]
    select: String,
    /// Namespace password, for a respcas namespace that has one.
    #[arg(short = 'w', long, default_value = "", global = true)]
    password: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// One command; the reply goes to stdout.
    Cmd(CmdArgs),
    /// A fixed-duration load; one TSV row goes to stdout.
    Bench(bench::BenchArgs),
}

#[derive(Args)]
struct CmdArgs {
    #[arg(long, value_enum, default_value = "text")]
    reply: ReplyFmt,
    #[arg(required = true, num_args = 1..)]
    args: Vec<String>,
}

#[derive(Copy, Clone, ValueEnum)]
enum ReplyFmt {
    /// Printable, with `(nil)` and `ERR-REPLY` markers.
    Text,
    /// The reply's bytes and nothing else, for byte comparison.
    Raw,
    /// Lowercase hex.
    Hex,
    /// The reply's length in bytes.
    Len,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let target = match &cli.unix {
        Some(p) => Target::Unix(p.clone()),
        None => Target::Tcp {
            host: cli.host.clone(),
            port: cli.port,
        },
    };

    let result = match cli.command {
        Command::Cmd(a) => do_cmd(target, &cli.select, &cli.password, a).await,
        Command::Bench(a) => bench::run(target, &cli.select, &cli.password, a).await,
    };

    match result {
        Ok(code) => ExitCode::from(code as u8),
        Err(e) => {
            eprintln!("respload: {e:#}");
            ExitCode::from(3)
        }
    }
}

async fn do_cmd(target: Target, select: &str, password: &str, a: CmdArgs) -> Result<i32> {
    let mut c = Conn::connect(&target).await?;
    if !select.is_empty()
        && let Reply::Error(e) = c.select(select, password).await?
    {
        bail!("SELECT {select} failed: {e}");
    }

    let args: Vec<Vec<u8>> = a
        .args
        .iter()
        .map(|s| decode_arg(s))
        .collect::<Result<_>>()?;
    let refs: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    let reply = c.call(&refs).await?;

    let mut out = std::io::stdout().lock();
    out.write_all(&render(&reply, a.reply))?;
    if !matches!(a.reply, ReplyFmt::Raw) {
        out.write_all(b"\n")?;
    }
    out.flush()?;

    // An error reply is a result, not a crash: exit 1 says the server answered
    // and said no, which a caller can tell apart from exit 3, could not ask.
    Ok(if reply.as_error().is_some() { 1 } else { 0 })
}

fn decode_arg(a: &str) -> Result<Vec<u8>> {
    if let Some(h) = a.strip_prefix("hex:") {
        if !h.len().is_multiple_of(2) {
            bail!("hex: argument has an odd number of digits");
        }
        return (0..h.len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(&h[i..i + 2], 16).with_context(|| format!("bad hex in {a}"))
            })
            .collect();
    }
    if let Some(p) = a.strip_prefix('@') {
        return std::fs::read(p).with_context(|| format!("read {p}"));
    }
    if let Some(n) = a.strip_prefix("rand:") {
        let n: usize = n.parse().with_context(|| format!("bad length in {a}"))?;
        let mut buf = vec![0u8; n];
        SplitMix64::new(seed_from_clock()).fill(&mut buf);
        return Ok(buf);
    }
    Ok(a.as_bytes().to_vec())
}

fn render(reply: &Reply, how: ReplyFmt) -> Vec<u8> {
    match reply {
        Reply::Array(items) => {
            let mut out = Vec::new();
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b'\n');
                }
                out.extend_from_slice(&render(item, how));
            }
            out
        }
        Reply::Nil => match how {
            ReplyFmt::Raw => Vec::new(),
            ReplyFmt::Len => b"0".to_vec(),
            _ => b"(nil)".to_vec(),
        },
        Reply::Int(i) => i.to_string().into_bytes(),
        Reply::Error(e) => match how {
            ReplyFmt::Raw => e.as_bytes().to_vec(),
            _ => format!("ERR-REPLY {e}").into_bytes(),
        },
        Reply::Bulk(b) | Reply::Simple(b) => match how {
            ReplyFmt::Hex => b
                .iter()
                .map(|x| format!("{x:02x}"))
                .collect::<String>()
                .into_bytes(),
            ReplyFmt::Len => b.len().to_string().into_bytes(),
            _ => b.clone(),
        },
    }
}
