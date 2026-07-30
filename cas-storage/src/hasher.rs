//! Block address hashing.
//!
//! A store hashes every 1 MiB block with exactly one algorithm at exactly one
//! width, both fixed at store creation and recorded in the store header. This
//! module turns that recorded (algo, width) pair into something that can hash.
//!
//! # Why an enum and not a trait
//!
//! This is deliberately a concrete enum rather than a `Hasher` trait:
//!
//! - Block hashing is one-shot over a fully buffered chunk, so there is no
//!   streaming state to abstract over and no `StreamingHasher` to write.
//! - Exhaustive matches are wanted. Every site that has to care about the
//!   width -- serialization, the header, the tools -- should stop compiling
//!   when a variant is added, instead of silently taking a default branch.
//! - Adding an algorithm is an enum variant plus a [`Hasher::from_header`]
//!   arm, not a new impl scattered behind a trait object, and it costs no
//!   dynamic dispatch on the hot write path.
//!
//! # MD5 is not here, by design
//!
//! MD5 survives in this codebase only as the `ContentHash`/ETag path, which is
//! an S3 protocol obligation over whole objects, not a block address. Block
//! addresses are BLAKE3. MD5 is not a variant of this enum and must never
//! become one: giving it an `algo_id` would make a collision-broken function
//! selectable as a content address.

use std::fmt::{self, Display, Formatter};

use crate::metastore::BlockId;

/// Algorithm id for BLAKE3 as written in the store header. Shared by both
/// widths -- the width is a separate header field, not a separate algorithm.
const ALGO_BLAKE3: u8 = 1;

/// Block address width in bytes for the truncated (128 bit) variant.
const WIDTH_16: u8 = 16;

/// Block address width in bytes for the full (256 bit) variant.
const WIDTH_32: u8 = 32;

/// The hash function a store uses to address its blocks.
///
/// Fixed for the lifetime of a store: it is written into the store header at
/// creation and reconstructed with [`Hasher::from_header`] on open. Blocks
/// written by one variant are not addressable by the other, which is why a
/// header mismatch is a refusal rather than a fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hasher {
    /// BLAKE3 truncated to the leading 16 bytes (128 bit addresses).
    Blake3W16,
    /// BLAKE3 at its full 32 byte output (256 bit addresses).
    Blake3W32,
}

impl Hasher {
    /// Returns the algorithm id this hasher records in the store header.
    ///
    /// Both widths report the same id: BLAKE3 truncated is still BLAKE3.
    pub fn algo_id(&self) -> u8 {
        match self {
            Hasher::Blake3W16 | Hasher::Blake3W32 => ALGO_BLAKE3,
        }
    }

    /// Returns the block address width in bytes: 16 or 32.
    pub fn width(&self) -> u8 {
        match self {
            Hasher::Blake3W16 => WIDTH_16,
            Hasher::Blake3W32 => WIDTH_32,
        }
    }

    /// Hashes a fully buffered block and returns its address.
    ///
    /// `Blake3W16` truncates the digest to its leading 16 bytes. Truncation is
    /// the standard way to derive a shorter BLAKE3 digest -- the output is an
    /// extendable stream whose prefixes are themselves valid digests -- so a
    /// 16 byte address is always the prefix of the 32 byte address of the same
    /// data.
    pub fn hash(&self, data: &[u8]) -> BlockId {
        let digest = blake3::hash(data);
        let full: [u8; WIDTH_32 as usize] = *digest.as_bytes();
        match self {
            Hasher::Blake3W16 => {
                let mut truncated = [0u8; WIDTH_16 as usize];
                truncated.copy_from_slice(&full[..WIDTH_16 as usize]);
                BlockId::from(truncated)
            }
            Hasher::Blake3W32 => BlockId::from(full),
        }
    }

    /// Rebuilds the hasher from the algorithm and width bytes of a store
    /// header.
    ///
    /// # Errors
    ///
    /// [`HasherError::UnknownAlgo`] if the algorithm id is not one this build
    /// knows, [`HasherError::UnsupportedWidth`] if the width is not one the
    /// algorithm supports. The store-header code turns both into its refusal
    /// errors; there is no fallback, because a store whose header cannot be
    /// interpreted cannot have its blocks addressed.
    pub fn from_header(algo: u8, width: u8) -> Result<Self, HasherError> {
        match algo {
            ALGO_BLAKE3 => match width {
                WIDTH_16 => Ok(Hasher::Blake3W16),
                WIDTH_32 => Ok(Hasher::Blake3W32),
                other => Err(HasherError::UnsupportedWidth(other)),
            },
            other => Err(HasherError::UnknownAlgo(other)),
        }
    }
}

/// Why a store header's (algo, width) pair could not be turned into a
/// [`Hasher`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HasherError {
    /// The algorithm id is not known to this build.
    UnknownAlgo(u8),
    /// The algorithm is known but does not support this address width.
    UnsupportedWidth(u8),
}

impl Display for HasherError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            HasherError::UnknownAlgo(algo) => {
                write!(f, "unknown block hash algorithm id {algo}")
            }
            HasherError::UnsupportedWidth(width) => {
                write!(f, "unsupported block hash width {width}")
            }
        }
    }
}

impl std::error::Error for HasherError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Official BLAKE3 vector for the empty input, from the reference
    /// test_vectors.json (extended output truncated to 32 bytes).
    const VEC_EMPTY: &str = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";

    /// Official BLAKE3 vector for the input `[0x00]` (input_len 1 of the
    /// reference test_vectors.json, whose input is the repeating 0..250
    /// pattern).
    const VEC_ZERO_BYTE: &str = "2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213";

    /// Published BLAKE3-256 digest of `b"abc"`.
    const VEC_ABC: &str = "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85";

    /// Inputs used by the property tests below.
    fn sample_inputs() -> Vec<Vec<u8>> {
        vec![
            Vec::new(),
            b"a".to_vec(),
            b"abc".to_vec(),
            b"the quick brown fox jumps over the lazy dog".to_vec(),
            vec![0u8; 1024],
            vec![0xffu8; 4096],
            (0..=255u8).cycle().take(1024 * 1024 + 7).collect(),
        ]
    }

    #[test]
    fn official_vectors_w32() {
        assert_eq!(Hasher::Blake3W32.hash(b"").to_hex(), VEC_EMPTY);
        assert_eq!(Hasher::Blake3W32.hash(&[0u8]).to_hex(), VEC_ZERO_BYTE);
        assert_eq!(Hasher::Blake3W32.hash(b"abc").to_hex(), VEC_ABC);
    }

    #[test]
    fn official_vectors_w16() {
        // The 16 byte address is the leading half of the official digest.
        assert_eq!(Hasher::Blake3W16.hash(b"").to_hex(), VEC_EMPTY[..32]);
        assert_eq!(Hasher::Blake3W16.hash(&[0u8]).to_hex(), VEC_ZERO_BYTE[..32]);
        assert_eq!(Hasher::Blake3W16.hash(b"abc").to_hex(), VEC_ABC[..32]);
    }

    #[test]
    fn truncation_is_prefix() {
        for input in sample_inputs() {
            let wide = Hasher::Blake3W32.hash(&input);
            let narrow = Hasher::Blake3W16.hash(&input);
            assert_eq!(
                narrow.as_slice(),
                &wide.as_slice()[..WIDTH_16 as usize],
                "W16 must be the prefix of W32 for input of len {}",
                input.len()
            );
        }
    }

    #[test]
    fn reports_algo_id_and_width() {
        assert_eq!(Hasher::Blake3W16.algo_id(), 1);
        assert_eq!(Hasher::Blake3W32.algo_id(), 1);
        assert_eq!(Hasher::Blake3W16.width(), 16);
        assert_eq!(Hasher::Blake3W32.width(), 32);
    }

    #[test]
    fn from_header_round_trip() {
        for hasher in [Hasher::Blake3W16, Hasher::Blake3W32] {
            let rebuilt = Hasher::from_header(hasher.algo_id(), hasher.width())
                .expect("own header bytes must round-trip");
            assert_eq!(rebuilt, hasher);
        }
    }

    #[test]
    fn from_header_rejects_unknown_algo() {
        assert_eq!(Hasher::from_header(0, 16), Err(HasherError::UnknownAlgo(0)));
        assert_eq!(Hasher::from_header(2, 32), Err(HasherError::UnknownAlgo(2)));
    }

    #[test]
    fn from_header_rejects_unsupported_width() {
        assert_eq!(
            Hasher::from_header(1, 8),
            Err(HasherError::UnsupportedWidth(8))
        );
        assert_eq!(
            Hasher::from_header(1, 17),
            Err(HasherError::UnsupportedWidth(17))
        );
        assert_eq!(
            Hasher::from_header(1, 0),
            Err(HasherError::UnsupportedWidth(0))
        );
    }

    #[test]
    fn block_ids_have_the_declared_width() {
        for input in sample_inputs() {
            assert_eq!(Hasher::Blake3W16.hash(&input).len(), 16);
            assert_eq!(Hasher::Blake3W32.hash(&input).len(), 32);
        }
    }

    #[test]
    fn block_id_equality_follows_content() {
        let a = Hasher::Blake3W32.hash(b"same bytes");
        let b = Hasher::Blake3W32.hash(b"same bytes");
        let c = Hasher::Blake3W32.hash(b"other bytes");
        assert_eq!(a, b);
        assert_ne!(a, c);

        // Same content, different width: different addresses, and the narrow
        // one never compares equal to the wide one despite the shared prefix.
        let narrow = Hasher::Blake3W16.hash(b"same bytes");
        assert_ne!(narrow, a);
        assert_eq!(narrow, Hasher::Blake3W16.hash(b"same bytes"));
    }

    #[test]
    fn error_messages_name_the_offending_byte() {
        assert_eq!(
            HasherError::UnknownAlgo(7).to_string(),
            "unknown block hash algorithm id 7"
        );
        assert_eq!(
            HasherError::UnsupportedWidth(24).to_string(),
            "unsupported block hash width 24"
        );
    }
}
