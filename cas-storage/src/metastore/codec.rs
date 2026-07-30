//! Byte-level helpers shared by the on-disk record formats (format v1).
//!
//! Every record is a flat little-endian byte string with no framing of its
//! own: the store hands back exactly the bytes that were written, so a record
//! decoder must treat both a short buffer and a long one as corruption.
//!
//! Two rules are implemented here once, rather than in every record:
//!
//! - All length and count fields are `u64`, independent of the host pointer
//!   width. They are converted to `usize` at the decode boundary, and a value
//!   that does not fit becomes `FsError::LengthOverflow` instead of a panic.
//! - Every derived offset is computed with checked arithmetic, so an absurd
//!   length field on disk produces `LengthOverflow` or `Truncated` rather than
//!   an overflow panic in a debug build.

use super::{BLOCKID_SIZE, BlockId, FsError, MAX_BLOCKID_SIZE};

/// Serializing a `usize` length as a `u64` is lossless only while `usize` is
/// at most 64 bits wide. Every supported target satisfies this; the assertion
/// makes the `as u64` casts below provably non-truncating.
const _: () = assert!(size_of::<usize>() <= size_of::<u64>());

/// Number of bytes an id list occupies: `id_width u8 | count u64 | ids`.
pub(crate) fn id_list_len(ids: &[BlockId]) -> usize {
    1 + 8 + ids.len() * ids.first().map_or(0, BlockId::len)
}

/// Appends a length or count field as a little-endian `u64`.
pub(crate) fn put_len(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(&(value as u64).to_le_bytes());
}

/// Appends a block-id list as `id_width u8 | count u64 | ids`.
///
/// The width byte makes the record self-describing, so a decoder never needs
/// to know which width the store that wrote it uses. All ids in one record
/// come from a single store and therefore share a width; an empty list writes
/// width 0, the only case in which 0 is a legal width.
pub(crate) fn put_id_list(out: &mut Vec<u8>, ids: &[BlockId]) {
    let width = ids.first().map_or(0, BlockId::len);
    debug_assert!(
        ids.iter().all(|id| id.len() == width),
        "block ids within one record must all have the same width"
    );
    // width is 0, BLOCKID_SIZE or MAX_BLOCKID_SIZE, all of which fit a u8.
    out.push(width as u8);
    put_len(out, ids.len());
    for id in ids {
        out.extend_from_slice(id.as_slice());
    }
}

/// A cursor over one record's bytes.
///
/// Reads advance the cursor and fail with `FsError::Truncated` the moment the
/// record is shorter than the field being read. `finish` closes the record and
/// rejects anything left over.
pub(crate) struct Reader<'a> {
    record: &'static str,
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(record: &'static str, buf: &'a [u8]) -> Self {
        Self {
            record,
            buf,
            pos: 0,
        }
    }

    /// Consumes `len` bytes, or reports why they are not there.
    fn take(&mut self, field: &'static str, len: usize) -> Result<&'a [u8], FsError> {
        let end = self.pos.checked_add(len).ok_or(FsError::LengthOverflow {
            record: self.record,
            field,
        })?;
        if end > self.buf.len() {
            return Err(FsError::Truncated {
                record: self.record,
                needed: end,
                got: self.buf.len(),
            });
        }
        let out = &self.buf[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    pub(crate) fn u8(&mut self, field: &'static str) -> Result<u8, FsError> {
        Ok(self.take(field, 1)?[0])
    }

    pub(crate) fn u16(&mut self, field: &'static str) -> Result<u16, FsError> {
        let raw: [u8; 2] = self.take(field, 2)?.try_into().unwrap();
        Ok(u16::from_le_bytes(raw))
    }

    pub(crate) fn u64(&mut self, field: &'static str) -> Result<u64, FsError> {
        let raw: [u8; 8] = self.take(field, 8)?.try_into().unwrap();
        Ok(u64::from_le_bytes(raw))
    }

    pub(crate) fn i64(&mut self, field: &'static str) -> Result<i64, FsError> {
        let raw: [u8; 8] = self.take(field, 8)?.try_into().unwrap();
        Ok(i64::from_le_bytes(raw))
    }

    /// Reads a length or count field: a `u64` on disk, a `usize` in memory.
    pub(crate) fn len(&mut self, field: &'static str) -> Result<usize, FsError> {
        usize::try_from(self.u64(field)?).map_err(|_| FsError::LengthOverflow {
            record: self.record,
            field,
        })
    }

    pub(crate) fn array<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<[u8; N], FsError> {
        Ok(self.take(field, N)?.try_into().unwrap())
    }

    pub(crate) fn bytes(&mut self, field: &'static str, len: usize) -> Result<&'a [u8], FsError> {
        self.take(field, len)
    }

    /// Reads `len` bytes and validates them as UTF-8.
    ///
    /// These bytes come back off disk, where the "we only ever wrote valid
    /// strings" invariant can be broken by corruption or a foreign format, so
    /// they are validated rather than assumed.
    pub(crate) fn utf8(&mut self, field: &'static str, len: usize) -> Result<String, FsError> {
        let raw = self.take(field, len)?;
        String::from_utf8(raw.to_vec()).map_err(|_| FsError::InvalidUtf8 {
            record: self.record,
            field,
        })
    }

    /// Reads a block-id list written by [`put_id_list`].
    ///
    /// The list length is derived exactly from the width byte and the count,
    /// so bytes past the last id stay in the record and are caught by
    /// [`Reader::finish`] rather than being absorbed as extra ids.
    pub(crate) fn id_list(&mut self) -> Result<Vec<BlockId>, FsError> {
        let width = self.u8("id_width")?;
        let count = self.len("id_count")?;
        let width = match width {
            // The only legal use of width 0: an empty list.
            0 if count == 0 => return Ok(Vec::new()),
            w if w as usize == BLOCKID_SIZE || w as usize == MAX_BLOCKID_SIZE => w as usize,
            w => return Err(FsError::InvalidIdWidth(w)),
        };
        let total = count.checked_mul(width).ok_or(FsError::LengthOverflow {
            record: self.record,
            field: "id_count",
        })?;
        let raw = self.take("ids", total)?;
        // `count` is bounded by the record length now that the bytes are in
        // hand, so this allocation cannot be inflated by the count field.
        let mut ids = Vec::with_capacity(count);
        for chunk in raw.chunks_exact(width) {
            ids.push(BlockId::from_slice(chunk)?);
        }
        Ok(ids)
    }

    /// Ends the record: anything after the last field is corruption.
    pub(crate) fn finish(self) -> Result<(), FsError> {
        if self.pos != self.buf.len() {
            return Err(FsError::TrailingBytes {
                record: self.record,
                extra: self.buf.len() - self.pos,
            });
        }
        Ok(())
    }
}
