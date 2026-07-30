//! Minimal async byte stream type for the write path.
//!
//! Replaces `rusoto_core::ByteStream` which used to be the library's
//! only leaked external-crate type. See PRD-003 for context.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;

/// A type-erased async stream of `Bytes` chunks.
///
/// This is the input type for [`CasFS::store_object`] and
/// [`CasFS::store_single_object_and_meta`]. Consumers build one by
/// wrapping any `Stream<Item = io::Result<Bytes>> + Send + 'static`.
pub struct AsyncByteStream {
    inner: Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + 'static>>,
}

impl AsyncByteStream {
    pub fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = io::Result<Bytes>> + Send + 'static,
    {
        Self {
            inner: Box::pin(stream),
        }
    }
}

impl Stream for AsyncByteStream {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}
