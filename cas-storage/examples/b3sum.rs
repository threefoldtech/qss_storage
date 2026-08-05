//! BLAKE3-256 of stdin, as hex. A test-harness tool, not a shipped binary.
//!
//! The real-hardware campaign needs to construct ADR 0014 mode B commands:
//! `SET <32-byte address> <value>`, where the client is the one that knows
//! the address. Without a hasher it can only ever replay an address the
//! server handed back, which tests the by-reference path and leaves the
//! first-write verification path -- the one that must REFUSE mismatched
//! bytes -- untestable.
//!
//! Deliberately the same `blake3` crate the store uses. This is not a second
//! implementation kept honest against the first; the hash function's own
//! correctness is upstream's test suite's job. What this checks is the
//! plumbing around it: that the address on the wire is the digest of the
//! bytes on the wire.
//!
//!     cargo build --release --example b3sum
//!     printf 'hello' | target/release/examples/b3sum

use std::io::{self, Read, Write};

fn main() -> io::Result<()> {
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut stdin = io::stdin().lock();
    loop {
        let n = stdin.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    writeln!(io::stdout(), "{}", hasher.finalize().to_hex())
}
