//! The campaign's content generator, one process per shard instead of four
//! processes per file. A test-harness tool, not a shipped binary.
//!
//! `tests/real/lib/gen.sh` derives every object's content from its key:
//! an AES-256-CTR keystream over zeros, keyed by sha256(seed NUL key), with
//! the counter block sha256(seed NUL key NUL "iv")[..16]. Correct, portable,
//! and about four forks per file -- two `sha256sum`, one `openssl`, one
//! `head`.
//!
//! That is invisible at 250 GiB per object and fatal at 1 KiB. The terabyte
//! phase's tiny band stages ten thousand 1 KiB files per shard, and measured
//! on the campaign rig it spent 71.5 seconds per shard at 8,283 forks per
//! second with the disk 95% idle: three million objects would have taken 5.8
//! hours to generate and a few minutes to store.
//!
//! This reads a manifest and writes every file in it from one process. The
//! bytes are identical to the shell generator's, deliberately and
//! testably -- the verification path re-derives content with gen_stream, so
//! a generator that differed by one byte would fail the campaign rather than
//! speed it up. `tests/real/lib/selftest.sh` compares the two.
//!
//!     qssgen --seed <seed> < manifest.tsv
//!
//! Manifest lines are `<path>\t<object-key>\t<size>`; the object key is what
//! the content is derived from, the path is where it lands. Blank lines and
//! `#` comments are ignored.
//!
//!     qssgen --seed s --stdout <object-key> <size>
//!
//! writes one object's bytes to stdout, which is the shape gen_stream has and
//! what the equivalence test compares against.

use std::io::{self, BufRead, BufWriter, Write};
use std::path::Path;

use aes::Aes256;
use aes::cipher::{KeyIvInit, StreamCipher};
use sha2::{Digest, Sha256};

type Aes256Ctr = ctr::Ctr128BE<Aes256>;

/// The AES key for an object key: sha256(seed NUL key).
fn content_key(seed: &str, object_key: &str) -> [u8; 32] {
    let mut input = Vec::with_capacity(seed.len() + object_key.len() + 1);
    input.extend_from_slice(seed.as_bytes());
    input.push(0);
    input.extend_from_slice(object_key.as_bytes());
    Sha256::digest(&input).into()
}

/// The counter block: the first 16 bytes of sha256(seed NUL key NUL "iv").
fn content_iv(seed: &str, object_key: &str) -> [u8; 16] {
    let mut input = Vec::with_capacity(seed.len() + object_key.len() + 4);
    input.extend_from_slice(seed.as_bytes());
    input.push(0);
    input.extend_from_slice(object_key.as_bytes());
    input.push(0);
    input.extend_from_slice(b"iv");
    let digest = Sha256::digest(&input);
    let mut iv = [0u8; 16];
    iv.copy_from_slice(&digest[..16]);
    iv
}

/// Writes `size` bytes of the object's keystream.
///
/// CTR over zeros, in 1 MiB chunks: a short read is a prefix of a long one,
/// which is what lets a ranged GET verify against the same generator.
fn write_content<W: Write>(out: &mut W, seed: &str, object_key: &str, size: u64) -> io::Result<()> {
    let key = content_key(seed, object_key);
    let iv = content_iv(seed, object_key);
    let mut cipher = Aes256Ctr::new(&key.into(), &iv.into());

    // Encrypting zeros IS the keystream, which is what `openssl enc
    // -aes-256-ctr -in /dev/zero` produces and therefore what every object
    // already in a campaign store was written from.
    const CHUNK: usize = 1 << 20;
    let mut buf = vec![0u8; CHUNK];

    let mut left = size;
    while left > 0 {
        let take = std::cmp::min(left, CHUNK as u64) as usize;
        buf[..take].fill(0);
        cipher.apply_keystream(&mut buf[..take]);
        out.write_all(&buf[..take])?;
        left -= take as u64;
    }
    Ok(())
}

fn write_file(path: &str, seed: &str, object_key: &str, size: u64) -> io::Result<()> {
    if let Some(parent) = Path::new(path).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(path)?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    write_content(&mut out, seed, object_key, size)?;
    out.flush()
}

fn usage() -> ! {
    eprintln!(
        "usage:\n  \
         qssgen --seed <seed> < manifest.tsv      (lines: <path>\\t<key>\\t<size>)\n  \
         qssgen --seed <seed> --stdout <key> <size>"
    );
    std::process::exit(2)
}

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut seed = String::from("qss-realtest");
    let mut to_stdout: Option<(String, u64)> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                i += 1;
                seed = args.get(i).unwrap_or_else(|| usage()).clone();
            }
            "--stdout" => {
                let key = args.get(i + 1).unwrap_or_else(|| usage()).clone();
                let size: u64 = args
                    .get(i + 2)
                    .unwrap_or_else(|| usage())
                    .parse()
                    .unwrap_or_else(|_| usage());
                to_stdout = Some((key, size));
                i += 2;
            }
            "--help" | "-h" => usage(),
            other => {
                eprintln!("qssgen: unexpected argument {other}");
                usage()
            }
        }
        i += 1;
    }

    if let Some((key, size)) = to_stdout {
        let stdout = io::stdout();
        let mut out = BufWriter::with_capacity(1 << 20, stdout.lock());
        write_content(&mut out, &seed, &key, size)?;
        return out.flush();
    }

    let stdin = io::stdin();
    let mut written = 0u64;
    for line in stdin.lock().lines() {
        let line = line?;
        let line = line.trim_end_matches('\n');
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split('\t');
        let (Some(path), Some(key), Some(size)) = (fields.next(), fields.next(), fields.next())
        else {
            return Err(io::Error::other(format!(
                "manifest line is not <path>\\t<key>\\t<size>: {line}"
            )));
        };
        let size: u64 = size
            .trim()
            .parse()
            .map_err(|_| io::Error::other(format!("size is not a number: {size}")))?;
        write_file(path, &seed, key, size)?;
        written += 1;
    }
    eprintln!("qssgen: {written} file(s)");
    Ok(())
}
