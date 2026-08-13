//! The load itself: N connections, each with a fixed pipeline depth, for a
//! fixed duration.
//!
//! It grades nothing. It moves bytes, reports what it moved, and leaves the
//! verdict to whoever reads the row -- the same division the campaign's shell
//! phases already draw, so the output here is the same fifteen-column TSV
//! `respcli.py bench` prints and the same `awk` reads it.
//!
//! Three choices in here decide whether the number means anything:
//!
//! - **Connections open before the clock starts.** Sixty-four TCP handshakes
//!   inside the measurement window would be charged to the server as latency
//!   it never spent.
//! - **Content is unique unless asked otherwise.** Every value carries a
//!   worker-and-counter stamp in its first sixteen bytes, so a content-addressed
//!   store ingests each one. `--dedup` removes the stamp and every value
//!   becomes the same bytes, which measures the dedup path instead. A driver
//!   that did not choose would report the average of two different systems.
//! - **A read counts the bytes that came back.** Crediting a miss with the
//!   value size it asked for reports read bandwidth for data nothing read;
//!   misses are counted and reported in their own column instead.
//!
//! `--verify` reads each written value back on the same connection and compares
//! it byte for byte. That is not a throughput mode -- it doubles the round
//! trips -- it is how a server that mangles high bytes or truncates at a NUL
//! gets caught while it is busy rather than while it is idle.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use clap::{Args, ValueEnum};
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::resp::{Conn, Reply, Target};
use crate::rng::{SplitMix64, seed_from_clock};

/// How many server-chosen keys one worker keeps for `--keys-out`.
const KEYS_KEPT: usize = 64;

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum Op {
    /// SET key value -- the ingest path.
    Set,
    /// GET key -- the read path.
    Get,
    /// CSET value, the server returns the content key (respcas, ADR 0014).
    Cset,
    /// SET "" value, the empty-key spelling of a content-addressed write.
    CsetEmpty,
    /// EXISTS key -- metadata only, no value moves.
    Exists,
    /// DEL key -- the delete path.
    Del,
}

impl Op {
    fn name(self) -> &'static str {
        match self {
            Op::Set => "set",
            Op::Get => "get",
            Op::Cset => "cset",
            Op::CsetEmpty => "cset-empty",
            Op::Exists => "exists",
            Op::Del => "del",
        }
    }

    fn writes_value(self) -> bool {
        matches!(self, Op::Set | Op::Cset | Op::CsetEmpty)
    }
}

#[derive(Clone, Debug, Args)]
pub struct BenchArgs {
    #[arg(long, value_enum, default_value = "set")]
    pub op: Op,
    /// How long to drive load, in seconds.
    #[arg(long, default_value_t = 10.0)]
    pub seconds: f64,
    /// Connections, each one a worker.
    #[arg(long, default_value_t = 8)]
    pub connections: usize,
    /// Commands in flight per connection before any reply is read.
    #[arg(long, default_value_t = 1)]
    pub pipeline: usize,
    /// Value size in bytes.
    #[arg(long, default_value_t = 4096)]
    pub size: usize,
    /// Key prefix; keys are <prefix>:<worker>:<counter>.
    #[arg(long, default_value = "bench")]
    pub prefix: String,
    /// For read ops: how many distinct keys per worker to cycle over.
    #[arg(long, default_value_t = 1000)]
    pub keyspace: u64,
    /// Write identical content, to measure the dedup path.
    #[arg(long)]
    pub dedup: bool,
    /// Read each written value back and compare it byte for byte.
    #[arg(long)]
    pub verify: bool,
    /// Seconds to wait on one socket operation before calling it an error.
    #[arg(long, default_value_t = 60.0)]
    pub timeout: f64,
    /// Content seed. 0 takes one from the clock, so two runs do not
    /// accidentally deduplicate against each other.
    #[arg(long, default_value_t = 0)]
    pub seed: u64,
    /// Write the first server-returned keys here, one hex string per line.
    #[arg(long, default_value = "")]
    pub keys_out: String,
    /// Exit 2 if any command errored.
    #[arg(long)]
    pub fail_on_error: bool,
}

#[derive(Default)]
struct Stats {
    ops: u64,
    bytes: u64,
    errors: u64,
    misses: u64,
    /// Seconds per command, one sample per pipeline batch.
    latencies: Vec<f64>,
    keys: Vec<String>,
    first_error: Option<String>,
}

impl Stats {
    fn fail(&mut self, what: String) {
        self.errors += 1;
        if self.first_error.is_none() {
            self.first_error = Some(what.replace(['\t', '\n', '\r'], " "));
        }
    }
}

/// Connect, drive, print one TSV row. The returned code is the process's.
pub async fn run(target: Target, select: &str, password: &str, o: BenchArgs) -> Result<i32> {
    if o.connections == 0 || o.pipeline == 0 {
        bail!("--connections and --pipeline must be at least 1");
    }
    if o.keyspace == 0 {
        bail!("--keyspace must be at least 1");
    }
    if o.verify && o.op != Op::Set {
        // CSET's key is chosen by the server, so the read-back cannot be
        // pipelined behind the write; GET/EXISTS/DEL have nothing to verify.
        bail!("--verify applies to --op set only");
    }
    if o.seconds <= 0.0 {
        bail!("--seconds must be positive");
    }

    // Every connection is up and in its namespace before the clock starts.
    let mut conns = Vec::with_capacity(o.connections);
    for i in 0..o.connections {
        let mut c = Conn::connect(&target).await?;
        if !select.is_empty()
            && let Reply::Error(e) = c.select(select, password).await?
        {
            bail!("connection {i}: SELECT {select} failed: {e}");
        }
        conns.push(c);
    }

    let seed = if o.seed == 0 {
        seed_from_clock()
    } else {
        o.seed
    };
    let o = Arc::new(o);
    let deadline = Instant::now() + Duration::from_secs_f64(o.seconds);
    let started = Instant::now();

    let mut set = JoinSet::new();
    for (wid, c) in conns.into_iter().enumerate() {
        let o = Arc::clone(&o);
        set.spawn(async move { worker(o, wid, seed, c, deadline).await });
    }

    let mut agg = Stats::default();
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(s) => {
                agg.ops += s.ops;
                agg.bytes += s.bytes;
                agg.errors += s.errors;
                agg.misses += s.misses;
                agg.latencies.extend(s.latencies);
                agg.keys.extend(s.keys);
                if agg.first_error.is_none() {
                    agg.first_error = s.first_error;
                }
            }
            // A worker that panicked is an error in the driver, not the
            // server, and saying so is more useful than a missing row.
            Err(e) => agg.fail(format!("worker panicked: {e}")),
        }
    }
    let elapsed = started.elapsed().as_secs_f64();

    agg.latencies
        .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let pct = |p: f64| -> f64 {
        if agg.latencies.is_empty() {
            return 0.0;
        }
        let i = ((agg.latencies.len() as f64) * p) as usize;
        agg.latencies[i.min(agg.latencies.len() - 1)] * 1000.0
    };

    println!(
        "op\tseconds\tops\tbytes\tops_per_s\tmib_per_s\terrors\t\
         p50_ms\tp95_ms\tp99_ms\tconnections\tpipeline\tvalue_bytes\tmisses\tfirst_error"
    );
    println!(
        "{}\t{:.3}\t{}\t{}\t{:.1}\t{:.2}\t{}\t{:.3}\t{:.3}\t{:.3}\t{}\t{}\t{}\t{}\t{}",
        o.op.name(),
        elapsed,
        agg.ops,
        agg.bytes,
        if elapsed > 0.0 {
            agg.ops as f64 / elapsed
        } else {
            0.0
        },
        if elapsed > 0.0 {
            agg.bytes as f64 / elapsed / 1_048_576.0
        } else {
            0.0
        },
        agg.errors,
        pct(0.50),
        pct(0.95),
        pct(0.99),
        o.connections,
        o.pipeline,
        o.size,
        agg.misses,
        agg.first_error.as_deref().unwrap_or(""),
    );

    if !o.keys_out.is_empty() {
        let body: String = agg.keys.iter().map(|k| format!("{k}\n")).collect();
        std::fs::write(&o.keys_out, body)?;
    }

    Ok(if agg.errors > 0 && o.fail_on_error {
        2
    } else {
        0
    })
}

/// One connection's share of the load. Failures are recorded, never returned:
/// a dead worker must not take the run's numbers down with it.
async fn worker(o: Arc<BenchArgs>, wid: usize, seed: u64, mut c: Conn, deadline: Instant) -> Stats {
    let mut st = Stats::default();
    let io = Duration::from_secs_f64(o.timeout);

    // Content is per worker, so no two workers write the same bytes unless
    // --dedup asks them to.
    let mut val = vec![0u8; o.size];
    SplitMix64::new(seed ^ (wid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)).fill(&mut val);

    let mut n: u64 = 0;

    'outer: while Instant::now() < deadline {
        let t0 = Instant::now();
        let mut batch: Vec<(u64, u64)> = Vec::with_capacity(o.pipeline);

        for _ in 0..o.pipeline {
            stamp(&mut val, wid, n, o.dedup);
            let key = key_for(&o, wid, n);
            let nbytes = match send_one(&mut c, &o, &key, &val, io).await {
                Ok(nbytes) => nbytes,
                Err(e) => {
                    st.fail(format!("send: {e}"));
                    break 'outer;
                }
            };
            batch.push((n, nbytes));
            n += 1;
        }

        match timeout(io, c.flush()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                st.fail(format!("flush: {e}"));
                break;
            }
            Err(_) => {
                st.fail(format!("flush timed out after {}s", o.timeout));
                break;
            }
        }

        for (bn, nbytes) in &batch {
            let rep = match timeout(io, c.reply()).await {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    st.fail(format!("reply: {e}"));
                    break 'outer;
                }
                Err(_) => {
                    st.fail(format!("reply timed out after {}s", o.timeout));
                    break 'outer;
                }
            };
            account(&mut st, &o, rep, *nbytes);

            if o.verify {
                let rep = match timeout(io, c.reply()).await {
                    Ok(Ok(r)) => r,
                    Ok(Err(e)) => {
                        st.fail(format!("verify reply: {e}"));
                        break 'outer;
                    }
                    Err(_) => {
                        st.fail(format!("verify reply timed out after {}s", o.timeout));
                        break 'outer;
                    }
                };
                stamp(&mut val, wid, *bn, o.dedup);
                verify(&mut st, &o, wid, *bn, &rep, &val);
            }
        }

        st.latencies
            .push(t0.elapsed().as_secs_f64() / o.pipeline as f64);
    }

    st
}

/// Queue the op's command (plus its read-back under `--verify`) and return the
/// value bytes it puts on the wire.
async fn send_one(c: &mut Conn, o: &BenchArgs, key: &str, val: &[u8], io: Duration) -> Result<u64> {
    let k = key.as_bytes();
    let (args, nbytes): (Vec<&[u8]>, u64) = match o.op {
        Op::Set => (vec![b"SET", k, val], val.len() as u64),
        Op::Cset => (vec![b"CSET", val], val.len() as u64),
        Op::CsetEmpty => (vec![b"SET", b"", val], val.len() as u64),
        Op::Get => (vec![b"GET", k], 0),
        Op::Exists => (vec![b"EXISTS", k], 0),
        Op::Del => (vec![b"DEL", k], 0),
    };
    match timeout(io, c.send(&args)).await {
        Ok(r) => r?,
        Err(_) => bail!("send timed out after {}s", o.timeout),
    }
    if o.verify {
        match timeout(io, c.send(&[b"GET", k])).await {
            Ok(r) => r?,
            Err(_) => bail!("verify send timed out after {}s", o.timeout),
        }
    }
    Ok(nbytes)
}

fn account(st: &mut Stats, o: &BenchArgs, rep: Reply, nbytes: u64) {
    if let Some(e) = rep.as_error() {
        st.fail(e.to_string());
        return;
    }
    st.ops += 1;
    if o.op == Op::Get {
        // A read moves the bytes that came back, not the bytes asked for.
        match rep.bytes() {
            Some(b) => st.bytes += b.len() as u64,
            None => st.misses += 1,
        }
        return;
    }
    st.bytes += nbytes;
    if (o.op == Op::Cset || o.op == Op::CsetEmpty)
        && st.keys.len() < KEYS_KEPT
        && let Some(b) = rep.bytes()
    {
        st.keys.push(hex(b));
    }
}

fn verify(st: &mut Stats, o: &BenchArgs, wid: usize, n: u64, rep: &Reply, want: &[u8]) {
    match rep {
        Reply::Error(e) => st.fail(format!("verify: {e}")),
        Reply::Nil => st.fail(format!("verify: {}:{wid}:{n} read back missing", o.prefix)),
        r => match r.bytes() {
            Some(got) if got == want => {}
            Some(got) => st.fail(format!(
                "verify: {}:{wid}:{n} read back {} bytes, wrote {}{}",
                o.prefix,
                got.len(),
                want.len(),
                if got.len() == want.len() {
                    ", content differs"
                } else {
                    ""
                }
            )),
            None => st.fail(format!(
                "verify: {}:{wid}:{n} read back a non-value reply",
                o.prefix
            )),
        },
    }
}

/// Values differ per worker and per command unless `--dedup`. The stamp is a
/// fixed sixteen bytes at the front, so each rewrite covers the last one
/// exactly and the random tail is never disturbed. A value shorter than the
/// stamp keeps as much of it as it has room for.
fn stamp(val: &mut [u8], wid: usize, n: u64, dedup: bool) {
    if dedup || val.is_empty() {
        return;
    }
    let mark = format!("{:08}{:08}", wid as u64 % 100_000_000, n % 100_000_000);
    let take = mark.len().min(val.len());
    val[..take].copy_from_slice(&mark.as_bytes()[..take]);
}

fn key_for(o: &BenchArgs, wid: usize, n: u64) -> String {
    if o.op.writes_value() {
        format!("{}:{}:{}", o.prefix, wid, n)
    } else {
        format!("{}:{}:{}", o.prefix, wid, n % o.keyspace)
    }
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        let _ = write!(s, "{byte:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(op: Op, prefix: &str, keyspace: u64) -> BenchArgs {
        BenchArgs {
            op,
            seconds: 1.0,
            connections: 1,
            pipeline: 1,
            size: 4096,
            prefix: prefix.to_string(),
            keyspace,
            dedup: false,
            verify: false,
            timeout: 60.0,
            seed: 1,
            keys_out: String::new(),
            fail_on_error: false,
        }
    }

    /// Unique content is the default, and it has to survive being rewritten in
    /// place: the stamp covers the previous one exactly, so nothing of an
    /// older value can be left in the tail.
    #[test]
    fn every_value_differs_and_the_tail_is_untouched() {
        let mut a = vec![7u8; 64];
        SplitMix64::new(99).fill(&mut a);
        let mut b = a.clone();

        stamp(&mut a, 0, 1, false);
        let tail_after_first = a[16..].to_vec();
        stamp(&mut b, 0, 2, false);

        assert_ne!(a[..16], b[..16], "two writes produced the same stamp");
        assert_eq!(tail_after_first, b[16..], "the random tail moved");

        stamp(&mut b, 0, 1, false);
        assert_eq!(a, b, "the same worker and counter produced different bytes");
    }

    /// `--dedup` is the whole dedup measurement: identical bytes every time.
    #[test]
    fn dedup_leaves_the_content_alone() {
        let mut v = vec![0u8; 64];
        SplitMix64::new(5).fill(&mut v);
        let before = v.clone();
        stamp(&mut v, 3, 77, true);
        assert_eq!(before, v);
    }

    /// A value with no room for the whole stamp keeps as much as it has, and a
    /// zero-byte value is left alone rather than panicking on a slice.
    #[test]
    fn short_values_are_stamped_as_far_as_they_reach() {
        let mut v = vec![0u8; 4];
        stamp(&mut v, 1, 2, false);
        assert_eq!(&v, b"0000");

        let mut empty: Vec<u8> = Vec::new();
        stamp(&mut empty, 1, 2, false);
        assert!(empty.is_empty());
    }

    /// Writes walk forward forever; reads cycle a bounded keyspace, because a
    /// read past what the writer reached measures the cost of a miss.
    #[test]
    fn reads_cycle_the_keyspace_and_writes_do_not() {
        let w = args(Op::Set, "p", 10);
        assert_eq!(key_for(&w, 2, 12), "p:2:12");

        let r = args(Op::Get, "p", 10);
        assert_eq!(key_for(&r, 2, 12), "p:2:2");
    }

    #[test]
    fn hex_is_lowercase_and_fixed_width() {
        assert_eq!(hex(&[0x00, 0x0f, 0xff]), "000fff");
    }

    /// The first failure is kept, later ones are only counted, and nothing in
    /// it can break the TSV row it lands in.
    #[test]
    fn the_first_error_is_kept_and_cannot_break_the_row() {
        let mut st = Stats::default();
        st.fail("first\tline\nsecond".into());
        st.fail("second".into());
        assert_eq!(st.errors, 2);
        assert_eq!(st.first_error.as_deref(), Some("first line second"));
    }
}
