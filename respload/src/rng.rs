//! SplitMix64, ten lines of it, instead of a dependency.
//!
//! The only thing the load needs randomness for is content a store cannot
//! compress or deduplicate by accident. SplitMix64 output is incompressible to
//! anything a block store does, and being seeded rather than drawn from the OS
//! buys something a CSPRNG would not: a run can be repeated byte for byte.
//!
//! The seed defaults to the clock rather than to zero, and that default is not
//! cosmetic. A fixed seed means the second run of `--op set` writes content the
//! store already holds, so a dedup-aware store answers from its index and the
//! run reports an ingest rate it never achieved. Repeatability has to be asked
//! for with `--seed`.

pub struct SplitMix64(u64);

impl SplitMix64 {
    pub fn new(seed: u64) -> SplitMix64 {
        SplitMix64(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    pub fn fill(&mut self, out: &mut [u8]) {
        for chunk in out.chunks_mut(8) {
            let v = self.next_u64().to_le_bytes();
            let n = chunk.len();
            chunk.copy_from_slice(&v[..n]);
        }
    }
}

/// A seed from the clock, for when the caller did not pick one.
pub fn seed_from_clock() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x1234_5678_9ABC_DEF0)
        | 1
}
