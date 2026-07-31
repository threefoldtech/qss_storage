use super::byte_stream::AsyncByteStream;
use super::fs::BLOCK_SIZE;
use futures::{Stream, ready};
use std::{
    io, mem,
    pin::Pin,
    task::{Context, Poll},
};

pub struct BufferedByteStream {
    // In a perfect world this would be an AsyncRead type, as that will likely be more performant
    // than reading bytes, and copying them. However the AyndRead implemented on this type is the
    // tokio one, which is not the same as the futures one. And I don't feel like adding a tokio
    // dependency here right now for that.
    // TODO: benchmark both approaches
    bs: AsyncByteStream,
    buffer: Vec<u8>,
    finished: bool,
}

impl BufferedByteStream {
    pub fn new(bs: AsyncByteStream) -> Self {
        Self {
            bs,
            buffer: Vec::with_capacity(BLOCK_SIZE),
            finished: false,
        }
    }
}

impl Stream for BufferedByteStream {
    type Item = io::Result<Vec<Vec<u8>>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        loop {
            match ready!(Pin::new(&mut self.bs).poll_next(cx)) {
                None => {
                    self.finished = true;
                    if !self.buffer.is_empty() {
                        // since we won't be using the vec anymore, we can replace it with a 0 capacity
                        // vec. This wont' allocate.
                        return Poll::Ready(Some(Ok(vec![mem::replace(
                            &mut self.buffer,
                            Vec::with_capacity(0),
                        )])));
                    }
                    return Poll::Ready(None);
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                Some(Ok(bytes)) => {
                    let mut buf_remainder = self.buffer.capacity() - self.buffer.len();
                    if bytes.len() < buf_remainder {
                        self.buffer.extend_from_slice(&bytes);
                    } else if bytes.len() == buf_remainder {
                        // The frame fills the buffer exactly. This condition
                        // once compared `self.buffer.len()` instead of
                        // `bytes.len()`, so a frame LARGER than the remainder
                        // arriving at a buffer that happened to be exactly
                        // half full was appended whole -- emitting a block
                        // larger than BLOCK_SIZE. See the regression test.
                        self.buffer.extend_from_slice(&bytes);
                        return Poll::Ready(Some(Ok(vec![mem::replace(
                            &mut self.buffer,
                            Vec::with_capacity(BLOCK_SIZE),
                        )])));
                    } else {
                        let mut out = Vec::with_capacity(
                            (bytes.len() - buf_remainder) / self.buffer.capacity() + 1,
                        );
                        self.buffer.extend_from_slice(&bytes[..buf_remainder]);
                        out.push(mem::replace(
                            &mut self.buffer,
                            Vec::with_capacity(BLOCK_SIZE),
                        ));
                        // repurpose buf_remainder as pointer to start of data
                        while bytes[buf_remainder..].len() > BLOCK_SIZE {
                            out.push(Vec::from(&bytes[buf_remainder..buf_remainder + BLOCK_SIZE]));
                            buf_remainder += BLOCK_SIZE;
                        }
                        // place the remainder in our buf
                        self.buffer.extend_from_slice(&bytes[buf_remainder..]);
                        return Poll::Ready(Some(Ok(out)));
                    };
                }
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::{StreamExt, stream};

    /// Collects every emitted block from frames fed through the stream.
    async fn blocks_from(frames: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        let s = AsyncByteStream::new(stream::iter(frames.into_iter().map(|f| Ok(Bytes::from(f)))));
        let mut buffered = BufferedByteStream::new(s);
        let mut out = Vec::new();
        while let Some(item) = buffered.next().await {
            out.extend(item.unwrap());
        }
        out
    }

    /// Regression test: a frame larger than the remaining buffer space,
    /// arriving while the buffer is exactly half full, used to be appended
    /// whole (the condition compared `self.buffer.len()` where `bytes.len()`
    /// was meant) and emitted a block larger than BLOCK_SIZE. The write path
    /// assumes no block exceeds BLOCK_SIZE.
    #[tokio::test]
    async fn oversized_frame_at_half_full_buffer_never_exceeds_block_size() {
        let half = BLOCK_SIZE / 2;
        // Frame 1 half-fills the buffer; frame 2 is bigger than the remainder
        // (and bigger than the buffered length, dodging both comparisons).
        let frames = vec![vec![1u8; half], vec![2u8; half + 100_000]];
        let total: usize = frames.iter().map(Vec::len).sum();

        let blocks = blocks_from(frames).await;

        for (i, block) in blocks.iter().enumerate() {
            assert!(
                block.len() <= BLOCK_SIZE,
                "block {i} is {} bytes, exceeding BLOCK_SIZE ({BLOCK_SIZE})",
                block.len()
            );
        }
        assert_eq!(
            blocks.iter().map(Vec::len).sum::<usize>(),
            total,
            "no bytes may be lost or duplicated"
        );
    }

    /// A frame that fills the buffer exactly emits exactly one full block.
    #[tokio::test]
    async fn exact_fill_emits_one_full_block() {
        let half = BLOCK_SIZE / 2;
        let blocks = blocks_from(vec![vec![1u8; half], vec![2u8; half]]).await;
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].len(), BLOCK_SIZE);
    }

    /// A single frame spanning several blocks is split at BLOCK_SIZE.
    #[tokio::test]
    async fn long_frame_is_split_at_block_size() {
        let blocks = blocks_from(vec![vec![3u8; 2 * BLOCK_SIZE + 123]]).await;
        assert_eq!(
            blocks.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![BLOCK_SIZE, BLOCK_SIZE, 123]
        );
    }
}
