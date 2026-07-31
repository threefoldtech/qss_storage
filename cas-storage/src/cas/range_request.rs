/// Requested bytes from a file.
#[derive(Debug)]
pub enum RangeRequest {
    /// All bytes, i.e. full file.
    All,
    /// A range of bytes from the file. The range is inclusive.
    Range(u64, u64),
    /// All bytes until the given position. This is equivalent to Range(0, value).
    ToBytes(u64),
    /// All bytes from a given position until the end of the file. This is equivalent to
    /// Range(value, EOF).
    FromBytes(u64),
}

impl RangeRequest {
    pub fn new_range(start: u64, end: u64) -> Self {
        RangeRequest::Range(start, end)
    }
}

// The header parser that used to live here is gone: it decoded a suffix
// range (`bytes=-500`, the LAST 500 bytes) as `ToBytes(500)`, the first
// 500. HTTP range headers are resolved by the caller against the object
// size (s3s's `Range::check`), and this type only carries the result.
