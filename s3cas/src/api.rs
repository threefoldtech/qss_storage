use std::io;

use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use tracing::error;

use cas_storage::CasFS;
use s3s::S3;
use s3s::S3Result;
use s3s::dto::StreamingBlob;
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, CompleteMultipartUploadInput,
    CompleteMultipartUploadOutput, CopyObjectInput, CopyObjectOutput, CreateBucketInput,
    CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput, DeleteBucketInput,
    DeleteBucketOutput, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput,
    DeleteObjectsOutput, GetBucketLocationInput, GetBucketLocationOutput, GetObjectInput,
    GetObjectOutput, HeadBucketInput, HeadBucketOutput, HeadObjectInput, HeadObjectOutput,
    ListBucketsInput, ListBucketsOutput, ListMultipartUploadsInput, ListMultipartUploadsOutput,
    ListObjectsInput, ListObjectsOutput, ListObjectsV2Input, ListObjectsV2Output, ListPartsInput,
    ListPartsOutput, PutObjectInput, PutObjectOutput, UploadPartInput, UploadPartOutput,
};
use s3s::s3_error;
use s3s::{S3Request, S3Response};

use crate::metrics::SharedMetrics;

const MAX_KEYS: i32 = 1000;
/// S3's ceiling on one `ListParts` page, and its default.
const MAX_PARTS: i32 = 1000;
/// S3's ceiling on one `ListMultipartUploads` page, and its default.
const MAX_UPLOADS: i32 = 1000;

pub struct S3Cas {
    casfs: CasFS,
    metrics: SharedMetrics,
}

impl std::fmt::Debug for S3Cas {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Cas").finish_non_exhaustive()
    }
}

impl S3Cas {
    pub fn new(casfs: CasFS, metrics: SharedMetrics) -> Self {
        match casfs.list_buckets() {
            Ok(buckets) => metrics.set_bucket_count(buckets.len()),
            Err(e) => error!("Could not count buckets for metrics: {}", e),
        }

        Self { casfs, metrics }
    }
}

/// Decode a client-supplied `Content-MD5` header: base64 of the raw 16-byte
/// MD5 digest. A header that does not decode to exactly 16 bytes is rejected
/// as `InvalidDigest`; comparing the digest against the received body is the
/// caller's job.
fn parse_content_md5(header: &str) -> S3Result<[u8; 16]> {
    use base64::Engine;
    let invalid = || s3_error!(InvalidDigest, "The Content-MD5 you specified is not valid.");
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(header)
        .map_err(|_| invalid())?;
    <[u8; 16]>::try_from(bytes).map_err(|_| invalid())
}

fn bad_digest() -> s3s::S3Error {
    s3_error!(
        BadDigest,
        "The Content-MD5 you specified did not match what we received."
    )
}

/// The length a streaming-signed chunked request declared, when no
/// `Content-Length` survived to the handler.
///
/// After decoding an aws-chunked body, s3s rewrites `Content-Length` to the
/// decoded size only if the header already exists (`get_mut`, never an
/// insert). A chunked upload that omits it entirely -- minio-go's shape for
/// a zero-byte PUT -- therefore reaches the handler with `content_length:
/// None` while the size the client signed sits in
/// `x-amz-decoded-content-length`. Falling back to that header extends it
/// exactly the trust the rewritten `Content-Length` already gets on the
/// non-empty path.
fn decoded_content_length(headers: &hyper::HeaderMap) -> Option<i64> {
    headers
        .get(s3s::header::X_AMZ_DECODED_CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

#[async_trait::async_trait]
impl S3 for S3Cas {
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        self.abort_multipart_upload_op(req).await
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        self.complete_multipart_upload_op(req).await
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        self.copy_object_op(req).await
    }

    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        self.create_bucket_op(req).await
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        self.create_multipart_upload_op(req).await
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.delete_bucket_op(req).await
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        self.delete_object_op(req).await
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        self.delete_objects_op(req).await
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        self.get_bucket_location_op(req).await
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        self.get_object_op(req).await
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.head_bucket_op(req).await
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        self.head_object_op(req).await
    }

    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        self.list_buckets_op(req).await
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        self.list_multipart_uploads_op(req).await
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        self.list_objects_op(req).await
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        self.list_objects_v2_op(req).await
    }

    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        self.list_parts_op(req).await
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        self.put_object_op(req).await
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        self.upload_part_op(req).await
    }
}

fn convert_stream_error(body: StreamingBlob) -> impl Stream<Item = Result<Bytes, io::Error>> {
    body.map(|r| r.map_err(|e| io::Error::other(e.to_string())))
}

mod buckets;
mod listing;
mod multipart;
mod objects;

#[cfg(test)]
mod tests {
    use super::decoded_content_length;
    use hyper::HeaderMap;

    #[test]
    fn a_zero_byte_streaming_put_finds_its_length_in_the_decoded_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            s3s::header::X_AMZ_DECODED_CONTENT_LENGTH,
            "0".parse().unwrap(),
        );
        assert_eq!(decoded_content_length(&headers), Some(0));
    }

    #[test]
    fn a_request_that_never_declared_a_length_still_has_none() {
        assert_eq!(decoded_content_length(&HeaderMap::new()), None);
        let mut headers = HeaderMap::new();
        headers.insert(
            s3s::header::X_AMZ_DECODED_CONTENT_LENGTH,
            "not-a-number".parse().unwrap(),
        );
        assert_eq!(decoded_content_length(&headers), None);
    }
}
