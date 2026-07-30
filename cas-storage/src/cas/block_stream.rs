use crate::hasher::Hasher;
use crate::metastore::BlockId;
use crate::metrics::SharedMetrics;

use super::range_request::RangeRequest;
use bytes::Bytes;
use futures::{AsyncRead, AsyncSeek, Future, Stream, ready};
use std::fmt::{self, Display, Formatter};
use std::{
    io,
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
};

/// How much is read per poll while buffering a block for verification.
const VERIFY_READ_CHUNK: usize = 64 * 1024;

/// A stored block whose bytes no longer hash to the address they are filed
/// under. Reported instead of the data when verification is on.
#[derive(Debug, Clone)]
pub struct BlockCorruption {
    expected: BlockId,
    actual: BlockId,
    path: PathBuf,
}

impl BlockCorruption {
    /// The address the block is filed under.
    pub fn expected(&self) -> &BlockId {
        &self.expected
    }

    /// What the bytes on disk actually hash to.
    pub fn actual(&self) -> &BlockId {
        &self.actual
    }

    /// The block file that failed the check.
    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl Display for BlockCorruption {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "corrupt block {}: file {} hashes to {}",
            self.expected.to_hex(),
            self.path.display(),
            self.actual.to_hex()
        )
    }
}

impl std::error::Error for BlockCorruption {}

impl From<BlockCorruption> for io::Error {
    fn from(err: BlockCorruption) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, err)
    }
}

/// What a verifying stream checks each block against: the store's hasher and
/// the block addresses of the object being read, in path order.
struct BlockVerification {
    hasher: Hasher,
    ids: Vec<BlockId>,
}

/// Implementation of a single stream over potentially multiple on disk data block files.
pub struct BlockStream {
    paths: Vec<(PathBuf, usize)>,
    fp: usize, // pointer to current file path
    size: usize,
    metrics: SharedMetrics,
    processed: usize,
    has_seeked: bool,
    range: RangeRequest,
    file: Option<async_fs::File>, // current file to read
    open_fut: Option<Pin<Box<dyn Future<Output = io::Result<async_fs::File>> + Send + Sync>>>,
    /// `Some` only when this stream verifies; see [`BlockStream::verified`].
    verify: Option<BlockVerification>,
    /// Index into `paths` of the file currently open. Only meaningful while
    /// verifying, which is the only mode that has to name the block it read.
    reading: usize,
    /// The block being buffered for verification. Empty otherwise.
    buf: Vec<u8>,
}

impl BlockStream {
    pub fn new(
        paths: Vec<(PathBuf, usize)>,
        size: usize,
        range: RangeRequest,
        metrics: SharedMetrics,
    ) -> Self {
        Self {
            paths,
            fp: 0,
            file: None,
            size,
            metrics,
            has_seeked: true,
            processed: 0,
            open_fut: None,
            range,
            verify: None,
            reading: 0,
            buf: Vec::new(),
        }
    }

    /// Re-hash every block before serving it, and fail the stream with a
    /// [`BlockCorruption`] error rather than hand out bytes that no longer
    /// match their address.
    ///
    /// `block_ids` are the object's block addresses in the same order as the
    /// paths this stream was built from, and `hasher` is the one the store
    /// addresses blocks with ([`crate::CasFS::hasher`]).
    ///
    /// This changes the memory behaviour of the stream: a verified block is
    /// read whole (a block is at most 1 MiB) and hashed before any of it is
    /// yielded, instead of being streamed out in small pieces.
    ///
    /// # Range requests are not verified
    ///
    /// A partial block cannot be checked against a whole-block address, so
    /// this is a no-op for anything but [`RangeRequest::All`]. Callers may
    /// therefore apply it unconditionally; a ranged read simply streams
    /// unverified, as documented on [`crate::CasFS::verify_on_read`].
    #[must_use]
    pub fn verified(mut self, hasher: Hasher, block_ids: Vec<BlockId>) -> Self {
        if matches!(self.range, RangeRequest::All) && block_ids.len() == self.paths.len() {
            self.verify = Some(BlockVerification {
                hasher,
                ids: block_ids,
            });
        } else {
            debug_assert!(
                block_ids.len() == self.paths.len(),
                "verification needs one block address per block file"
            );
        }
        self
    }

    /// Hash the block just buffered, compare it to the address it is filed
    /// under, and either yield it whole or report the corruption.
    fn finish_verified_block(&mut self) -> Poll<Option<io::Result<Bytes>>> {
        let bytes = std::mem::take(&mut self.buf);
        let verify = self
            .verify
            .as_ref()
            .expect("only reached while verification is on");
        let idx = self.reading;
        let Some(expected) = verify.ids.get(idx) else {
            return Poll::Ready(Some(Err(io::Error::other(format!(
                "no block address to verify block {idx} against"
            )))));
        };
        let actual = verify.hasher.hash(&bytes);
        if actual != *expected {
            let err = BlockCorruption {
                expected: *expected,
                actual,
                path: self.paths[idx].0.clone(),
            };
            tracing::error!(error = %err, "Refusing to serve corrupt block");
            return Poll::Ready(Some(Err(err.into())));
        }
        self.processed += bytes.len();
        self.metrics.bytes_sent(bytes.len());
        Poll::Ready(Some(Ok(bytes.into())))
    }
}
// ---- tfstor-extension: BEGIN ----
// `unsafe impl Sync for BlockStream {}` was here and has been DELETED. It came
// with no justification, and it turns out it never needed one: every field of
// `BlockStream` is already `Sync` -- `Vec<(PathBuf, usize)>`, plain integers and
// a `bool`, `RangeRequest`, the `Arc`-backed `SharedMetrics`, `async_fs::File`,
// and `open_fut`, whose boxed future is declared `+ Send + Sync` in the struct.
// So the compiler hands out an ordinary auto `Sync` impl and the workspace
// builds with the assertion gone.
//
// The static assertion below keeps that honest: if someone adds a field that is
// not `Sync` (a `Cell`, an `Rc`, a future without the `Sync` bound), this line
// fails the build at the definition, which is where the decision belongs --
// rather than the old behaviour, where an `unsafe impl` would have silently
// asserted the new field's soundness on the author's behalf.
const _: () = {
    const fn assert_sync<T: Sync>() {}
    assert_sync::<BlockStream>();
};
// ---- tfstor-extension: END ----

impl Stream for BlockStream {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (start, end) = match self.range {
            RangeRequest::Range(start, end) => (start, end),
            RangeRequest::ToBytes(end) => (0, end),
            RangeRequest::FromBytes(start) => (start, self.size as u64 + start),
            RangeRequest::All => (0, self.size as u64),
        };
        let processed = self.processed as u64;

        if processed >= end {
            // we did all we need here, exit. This is here because we can't both return data in the
            // actual read, and indicate the stream is done
            return Poll::Ready(None);
        }

        // try to seek in the file to the correct offset
        // since we skip files we don't need to read from, this file always has at least _some_
        // bytes to read, and hence seek is always within bounds (even though it is technically not
        // an error if it isn't).
        if !self.has_seeked
            && start > processed
            && let Some(ref mut file) = self.file
        {
            // `start` comes from a client Range header and is not clamped to
            // the object size, so the skip distance must be checked before it
            // is narrowed for the seek and the `processed` bookkeeping.
            let skip = start - processed;
            let (Ok(skip_seek), Ok(skip_len)) = (i64::try_from(skip), usize::try_from(skip)) else {
                return Poll::Ready(Some(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "range start does not fit the platform address space",
                ))));
            };
            return match Pin::new(file).poll_seek(cx, io::SeekFrom::Current(skip_seek)) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                Poll::Ready(Ok(_)) => {
                    self.has_seeked = true;
                    // TODO: this can be `n`
                    self.processed += skip_len;
                    self.poll_next(cx)
                }
            };
        }

        // verifying reader: buffer the whole block, then hash it before any of
        // it is handed out. `processed` only advances once a block passes,
        // which is what keeps the whole-block accounting (and the exit
        // condition above) intact.
        if self.verify.is_some()
            && let Some(ref mut file) = self.file
        {
            let mut buf = vec![0; VERIFY_READ_CHUNK];
            return match Pin::new(file).poll_read(cx, &mut buf) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                Poll::Ready(Ok(0)) => {
                    self.file = None;
                    self.finish_verified_block()
                }
                Poll::Ready(Ok(n)) => {
                    self.buf.extend_from_slice(&buf[..n]);
                    self.poll_next(cx)
                }
            };
        }

        // if we have an open file, try to read it
        if let Some(ref mut file) = self.file {
            // `end` is client-controlled and unclamped: saturate so
            // `end == u64::MAX` cannot overflow, and the min bounds the
            // narrowing cast.
            let cap = (end - processed).saturating_add(1).min(4096);
            let mut buf = vec![0; cap as usize];
            return match Pin::new(file).poll_read(cx, &mut buf) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                Poll::Ready(Ok(0)) => {
                    self.file = None;
                    self.poll_next(cx)
                }
                Poll::Ready(Ok(n)) => {
                    self.processed += n;
                    buf.truncate(n);
                    self.metrics.bytes_sent(n);
                    Poll::Ready(Some(Ok(buf.into())))
                }
            };
        }

        // check if we even need bytes from the next files
        // make sure to only do this when we are not already polling. The issue is that opening a
        // file advanced fp even before the file is opened, which might cause an issue in the
        // calculation of a range request, if the new file is so small that it would be skipped.
        if self.open_fut.is_none() {
            loop {
                // TODO: Fix this crap
                let processed = self.processed as u64;
                match self.range {
                    RangeRequest::Range(start, end) => {
                        if processed > end {
                            return Poll::Ready(None);
                        } else if processed < start {
                            if processed + (self.paths[self.fp].1 as u64) < start {
                                // skip file entirely
                                self.processed += self.paths[self.fp].1;
                                self.fp += 1;
                                if self.fp > self.paths.len() {
                                    return Poll::Ready(None);
                                }
                                continue;
                            }
                            break;
                        } else {
                            break;
                        }
                    }
                    RangeRequest::ToBytes(end) => {
                        if processed > end {
                            return Poll::Ready(None);
                        }
                        break;
                    }
                    RangeRequest::FromBytes(start) => {
                        if processed < start && processed + (self.paths[self.fp].1 as u64) < start {
                            // skip file entirely
                            self.processed += self.paths[self.fp].1;
                            self.fp += 1;
                            if self.fp > self.paths.len() {
                                return Poll::Ready(None);
                            }
                            continue;
                        }
                        break;
                    }
                    RangeRequest::All => break,
                }
            }
        }

        // we don't have an open file, check if we have any more left
        if self.fp > self.paths.len() {
            return Poll::Ready(None);
        }

        // try to open the next file
        // if we are not opening one already start doing so
        if self.open_fut.is_none() {
            self.open_fut = Some(Box::pin(async_fs::File::open(
                self.paths[self.fp].0.clone(),
            )));
            // remember which block this is before the pointer moves on: a
            // verified block has to be named if it fails the check
            self.reading = self.fp;
            // increment the file pointer for the next file
            self.fp += 1;
        };

        // this will always happen
        if let Some(ref mut open_fut) = self.open_fut {
            let file_res = ready!(open_fut.as_mut().poll(cx));
            // we opened a file, or there is an error
            // clear the open fut as it is done
            self.open_fut = None;
            match file_res {
                // if there is an error, we just return that. The next poll call will try to open the
                // next file
                Err(e) => return Poll::Ready(Some(Err(e))),
                // if we do have an open file, set it as open file, and immediately poll again to try
                // and read from it
                Ok(file) => {
                    self.file = Some(file);
                    self.has_seeked = false;
                    return self.poll_next(cx);
                }
            };
        };

        unreachable!();
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size))
    }
}
