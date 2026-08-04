//! `qss-storage-fsck`: offline reconciliation, scrub and repair for a
//! qss_storage block store (ADR 0005).
//!
//! The tool is thin on purpose. Every walker, pass and repair action lives in
//! `cas_storage::scrub`, so the daemon's own store is checked by the same code
//! an operator runs and a future online mode can wrap it; this crate is the
//! CLI, the store open, and the exit-code contract.
//!
//! Three properties are load-bearing here rather than in the library:
//!
//! - **It never creates a store.** `MetaStore::open_or_create` creates one at
//!   whatever path it is handed, so a mistyped `--meta-root` would produce an
//!   empty store and a clean report about nothing -- the loudest possible
//!   wrong answer from a tool whose job is finding damage. Both databases are
//!   classified before anything is constructed.
//! - **Report before repair.** The report is written AND flushed before the
//!   first repair action runs (ADR 0005 hard rule 2), so a `--repair` that
//!   dies halfway still leaves the operator the evidence.
//! - **Exclusivity is inherited, not built.** fjall's LOCK file makes this
//!   tool and a running daemon mutually exclusive on every database it opens.
//!   A store the daemon has open fails at the open, which is the intended
//!   answer.
//!
//! The pairing check (ADR 0012) runs before any pass, and not because this
//! file calls it: `SharedBlockStore::new` compares the header's store id with
//! the blocks root's `.store-id` marker while opening, so a mispaired store
//! never reaches a walker. That ordering is the point -- a store whose two
//! halves belong to different stores must not be "repaired" toward either
//! side's fiction. It surfaces here as a could-not-run (exit 3) naming both
//! ids and both paths, and `--re-pair` is the only way to make such a pairing
//! official.
//!
//! Exit codes (the scripting contract): 0 clean or INFO-only, 1 WARN, 2
//! CRITICAL, 3 could-not-run.

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use clap::Parser;

use cas_storage::config;
use cas_storage::metastore::store_header::{StoreInit, classify_db_dir};
use cas_storage::scrub::{RepairContext, RepairSummary, Report, ScrubOptions, exit_code, repair};
use cas_storage::{CasFS, Durability, SharedMetrics, StorageEngine, StoreOptions};

/// Help text for `--config`, the same wording the s3cas subcommands use.
const CONFIG_HELP: &str = "Path to qss_storage.toml (default: ./qss_storage.toml, then \
                           /etc/qss_storage/qss_storage.toml)";

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Offline reconciliation, scrub and repair for a qss_storage block store",
    long_about = "Walks a store's holders, block records and block files, reports what \
                  disagrees, and (with --repair) fixes what is safely fixable.\n\n\
                  The store must not be in use: fjall's LOCK file makes this tool and a \
                  running daemon mutually exclusive.\n\n\
                  Exit codes: 0 clean or informational only, 1 inconsistencies, 2 loss or \
                  loss-risk, 3 could not run."
)]
struct Cli {
    #[arg(long, help = CONFIG_HELP)]
    config: Option<PathBuf>,

    // Not clap defaults: --re-pair has to be able to tell "the operator said
    // this path" from "nobody said anything and the tool guessed .".
    #[arg(long, help = "Metadata root: the databases live here (default: .)")]
    meta_root: Option<PathBuf>,

    #[arg(long, help = "Data root: the block files live here (default: .)")]
    fs_root: Option<PathBuf>,

    #[arg(long, help = "Metadata DB (fjall); default fjall")]
    metadata_db: Option<StorageEngine>,

    #[arg(
        long,
        help = "Durability level (buffer, fsync); default fsync, which is strongest"
    )]
    durability: Option<Durability>,

    #[arg(long, help = "leave empty to disable it")]
    inline_metadata_size: Option<usize>,

    #[arg(
        long,
        help = "Also re-hash every block file against its own name (disk-bound: hours on a \
                large store)"
    )]
    scrub: bool,

    #[arg(
        long,
        help = "Apply the repairs the findings call for. The report is emitted first, and \
                every pass runs again afterwards"
    )]
    repair: bool,

    #[arg(long, help = "Emit the report (and the repair summary) as JSON")]
    json: bool,

    #[arg(
        long = "re-pair",
        conflicts_with_all = ["repair", "scrub", "json"],
        help = "Rewrite the blocks root's store-id marker to match the blocks database, making \
                a refused pairing official. Requires --meta-root and --fs-root spelled out; \
                runs no passes"
    )]
    re_pair: bool,
}

/// The default both roots take when the operator names neither.
fn or_here(path: Option<PathBuf>) -> PathBuf {
    path.unwrap_or_else(|| PathBuf::from("."))
}

/// `--re-pair`: the authoritative recovery from a refused open (ADR 0012).
///
/// Both roots must be spelled out. The verb rewrites one of them, and a
/// default of `.` is never a pairing anybody meant -- an operator who has
/// just been told two paths disagree should retype both of them.
fn run_re_pair(cli: &Cli) -> Result<u8> {
    let (Some(meta_root), Some(fs_root)) = (cli.meta_root.as_ref(), cli.fs_root.as_ref()) else {
        bail!(
            "--re-pair needs both --meta-root and --fs-root spelled out: it rewrites the store \
             id marker under --fs-root to match the database under --meta-root, and neither \
             path may be a default"
        );
    };

    let done = cas_storage::scrub::re_pair(meta_root, fs_root)?;
    emit(&done.render_text());
    Ok(exit_code::CLEAN)
}

/// The two databases a `--meta-root` holds, the way `CasFS::single_namespace`
/// builds them: the namespace DB and the shared blocks DB.
fn store_databases(meta_root: &Path) -> [PathBuf; 2] {
    [
        meta_root.join("db"),
        meta_root
            .join("blocks")
            .join(cas_storage::BLOCKS_DB_DIR_NAME),
    ]
}

/// Refuses a path that is not already a store.
///
/// Checked before anything is constructed, because constructing is what
/// creates: `MetaStore::open_or_create` would stamp a fresh header onto a
/// mistyped path and then report it clean. Every other tool that opens a store
/// refuses the same way -- `s3cas inspect` per database, `s3cas check` and
/// `retrieve` through `s3cas::inspect::refuse_unless_store_exists`, which is
/// this function for the crate next door.
///
/// # Errors
///
/// If either database is missing, naming the path that is not there.
fn refuse_unless_store_exists(meta_root: &Path) -> Result<()> {
    for db in store_databases(meta_root) {
        if classify_db_dir(&db)? == StoreInit::Create {
            bail!("no store at {}", db.display());
        }
    }
    Ok(())
}

/// Writes `text` to stdout and flushes it, tolerating a reader that has gone
/// away.
///
/// `println!` panics on a broken pipe, and `qss-storage-fsck --json | head`
/// is a thing an operator does. The flush is not optional either: the report
/// must be on the wire before `--repair` starts mutating (hard rule 2), and
/// stdout is block-buffered when it is a pipe.
fn emit(text: &str) {
    let mut out = io::stdout().lock();
    let written = out.write_all(text.as_bytes()).and_then(|()| out.flush());
    if let Err(e) = written
        && e.kind() != io::ErrorKind::BrokenPipe
    {
        let _ = writeln!(io::stderr(), "qss-storage-fsck: writing the report: {e}");
    }
}

/// Renders a report the way the flags ask for.
fn emit_report(report: &Report, json: bool) -> Result<()> {
    if json {
        emit(&format!("{}\n", serde_json::to_string_pretty(report)?));
    } else {
        emit(&report.render_text());
    }
    Ok(())
}

/// Renders a repair summary the way the flags ask for.
fn emit_summary(summary: &RepairSummary, json: bool) -> Result<()> {
    if json {
        emit(&format!("{}\n", serde_json::to_string_pretty(summary)?));
    } else {
        emit(&summary.render_text());
    }
    Ok(())
}

/// Opens the store, runs the passes, emits, and (if asked) repairs.
///
/// Returns the exit code rather than calling `exit` itself, so the one place
/// that terminates the process is `main`.
async fn run(cli: Cli) -> Result<u8> {
    let (file, source) = config::load(cli.config.as_deref())?;
    if let Some(path) = source {
        // Which file the run is on is the first thing to check when a tool
        // reports on a store nobody expected; stderr, so --json stays clean.
        let _ = writeln!(io::stderr(), "configuration loaded from {}", path.display());
    }

    // The one verb that walks nothing: it repairs the identity a walk would
    // not be allowed to run against.
    if cli.re_pair {
        return run_re_pair(&cli);
    }

    let meta_root = or_here(cli.meta_root);
    let fs_root = or_here(cli.fs_root);
    // No --stripe-count flag here: the stripe count is a write-concurrency
    // knob and fsck's repair path deletes serially. The config file's value is
    // still honoured so this opens the store the same way the daemon does.
    let store = StoreOptions::resolve(
        cli.metadata_db,
        cli.durability,
        cli.inline_metadata_size,
        None,
        &file.store,
    )?;

    refuse_unless_store_exists(&meta_root)?;

    // Opening is also the store's own recovery: the block writer purges
    // `.tmp` residue and re-checks that the temp dir shares a filesystem with
    // the blocks root. Both happen here for free, before the first walk.
    let casfs = CasFS::single_namespace(
        fs_root,
        meta_root.clone(),
        SharedMetrics::default(),
        StoreOptions {
            // verify_on_read is off whatever the config says: fsck never
            // reads an object through the read path, and the corruption
            // scrub re-hashes every block itself under --scrub.
            verify_on_read: false,
            // No commit station whatever the config says (ADR 0011): fsck
            // never goes through the write path, so a station here would be a
            // committer task waiting on a queue nobody feeds.
            group_commit: None,
            ..store
        },
    )?;

    let options = if cli.scrub {
        ScrubOptions::full()
    } else {
        ScrubOptions::metadata_only()
    };
    let ctx = RepairContext::new(&casfs).with_meta_root(meta_root);

    let report = cas_storage::scrub::run(&ctx.scrub_context(), &options)?;
    emit_report(&report, cli.json)?;

    if !cli.repair {
        return Ok(report.exit_code);
    }

    // Same options as the report: the post-repair passes have to look at what
    // the repair touched, and --scrub is the only pass that is optional.
    let summary = repair(&ctx, &report, &options).await?;
    emit_summary(&summary, cli.json)?;
    Ok(summary.exit_code())
}

#[tokio::main]
async fn main() {
    let code = match run(Cli::parse()).await {
        Ok(code) => code,
        Err(e) => {
            // Could-not-run, always: a store that will not open or will not
            // walk has no report, and 3 is how a script tells that from a
            // store that is merely damaged.
            let _ = writeln!(io::stderr(), "qss-storage-fsck: {e:#}");
            exit_code::COULD_NOT_RUN
        }
    };
    // stdout was flushed by `emit`; this is belt and braces for anything a
    // future path prints without it.
    let _ = io::stdout().flush();
    std::process::exit(i32::from(code));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both databases are checked, and they are the two paths the store
    /// actually builds -- checking only `<meta_root>/db` would pass a store
    /// whose block metadata is missing.
    #[test]
    fn a_missing_store_is_refused_by_the_path_that_is_missing() {
        let dir = tempfile::TempDir::new().unwrap();
        let err = refuse_unless_store_exists(dir.path()).unwrap_err();
        assert!(err.to_string().contains("no store at"), "{err}");
        assert!(
            err.to_string().ends_with("db"),
            "the message names the database path: {err}"
        );

        let dbs = store_databases(dir.path());
        assert!(dbs[0].ends_with("db"));
        assert!(dbs[1].ends_with("blocks/.db"));
    }
}
