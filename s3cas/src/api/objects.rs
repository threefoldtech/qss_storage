use s3s::S3Result;
use s3s::dto::StreamingBlob;
use s3s::dto::Timestamp;
use s3s::dto::{
    CopyObjectInput, CopyObjectOutput, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput,
    DeleteObjectsOutput, DeletedObject, ETag, GetObjectInput, GetObjectOutput, HeadObjectInput,
    HeadObjectOutput, PutObjectInput, PutObjectOutput,
};
use s3s::s3_error;
use s3s::{S3Request, S3Response};
use tracing::{error, info};

use cas_storage::AsyncByteStream;
use cas_storage::BlockStream;
use cas_storage::RangeRequest;
use md5::{Digest, Md5};

use super::S3Cas;
use super::{bad_digest, convert_stream_error, decoded_content_length, parse_content_md5};

fn fmt_content_range(start: u64, end_inclusive: u64, size: u64) -> String {
    format!("bytes {start}-{end_inclusive}/{size}")
}

impl S3Cas {
    pub(super) async fn get_object_op(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        info!("GET object {:?}", input);

        let GetObjectInput {
            bucket, key, range, ..
        } = input;

        if !try_!(self.casfs.bucket_exists(&bucket)) {
            return Err(s3_error!(NoSuchBucket, "Bucket does not exist"));
        }

        // load metadata

        let (obj_meta, paths) = match self.casfs.get_object_paths(&bucket, &key) {
            Ok(Some((obj_meta, paths))) => (obj_meta, paths),
            Ok(None) => {
                return Err(s3_error!(NoSuchKey, "Object does not exist"));
            }
            Err(e) => {
                error!("Could not get object metadata: {}", e);
                return Err(s3_error!(ServiceUnavailable, "service unavailable"));
            }
        };

        // The advertised headers and the returned body must come from the
        // same arithmetic, so the range is resolved against the object size
        // before anything is built from it. `check` clamps the end, handles
        // suffix ranges, and turns an unsatisfiable range into a 416 --
        // before a body exists. s3s answers 206 exactly when content_range
        // is set, so it is only set for an actual ranged request.

        // if the object is inlined, we return it directly
        if let Some(data) = obj_meta.inlined() {
            let full_size = data.len() as u64;
            let (bytes, content_range) = match range {
                Some(ref range) => {
                    let resolved = range.check(full_size)?;
                    (
                        bytes::Bytes::from(data.clone())
                            .slice(resolved.start as usize..resolved.end as usize),
                        Some(fmt_content_range(
                            resolved.start,
                            resolved.end - 1,
                            full_size,
                        )),
                    )
                }
                None => (bytes::Bytes::from(data.clone()), None),
            };

            let content_length = bytes.len() as i64;
            let body = s3s::Body::from(bytes);
            let stream = StreamingBlob::from(body);

            let output = GetObjectOutput {
                body: Some(stream),
                content_length: Some(content_length),
                content_range,
                accept_ranges: Some("bytes".to_string()),
                last_modified: Some(Timestamp::from(obj_meta.last_modified())),
                e_tag: Some(ETag::Strong(obj_meta.format_e_tag())),
                ..Default::default()
            };
            return Ok(S3Response::new(output));
        }

        let full_size = obj_meta.size();
        let resolved = match range {
            Some(ref range) => Some(range.check(full_size)?),
            None => None,
        };
        let (range_request, stream_size, content_range) = match resolved {
            Some(r) => (
                RangeRequest::new_range(r.start, r.end - 1),
                r.end - r.start,
                Some(fmt_content_range(r.start, r.end - 1, full_size)),
            ),
            None => (RangeRequest::All, full_size, None),
        };

        let block_size: usize = paths.iter().map(|(_, size)| size).sum();

        debug_assert!(obj_meta.size() == block_size as u64);
        let whole_object = matches!(range_request, RangeRequest::All);
        let mut block_stream =
            BlockStream::new(paths, block_size, range_request, self.metrics.to_cas());
        if self.casfs.verify_on_read() && whole_object {
            // Whole reads only: a partial block cannot be checked against a
            // whole-block address, and the verifying reader buffers and
            // yields entire blocks, which would widen a ranged body.
            block_stream = block_stream.verified(self.casfs.hasher(), obj_meta.blocks().to_vec());
        }
        let stream = StreamingBlob::wrap(block_stream);

        let output = GetObjectOutput {
            body: Some(stream),
            content_length: Some(stream_size as i64),
            content_range,
            accept_ranges: Some("bytes".to_string()),
            last_modified: Some(Timestamp::from(obj_meta.last_modified())),
            //metadata: object_metadata,
            e_tag: Some(ETag::Strong(obj_meta.format_e_tag())),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    pub(super) async fn put_object_op(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let decoded_content_length = decoded_content_length(&req.headers);
        let input = req.input;
        info!("PUT object {:?}", input);
        if let Some(ref storage_class) = input.storage_class {
            let is_valid = ["STANDARD", "REDUCED_REDUNDANCY"].contains(&storage_class.as_str());
            if !is_valid {
                return Err(s3_error!(InvalidStorageClass));
            }
        }

        let PutObjectInput {
            body,
            bucket,
            key,
            content_length,
            content_md5,
            ..
        } = input;

        let expected_md5 = content_md5.as_deref().map(parse_content_md5).transpose()?;

        let Some(body) = body else {
            return Err(s3_error!(IncompleteBody));
        };

        if !try_!(self.casfs.bucket_exists(&bucket)) {
            return Err(s3_error!(NoSuchBucket, "Bucket does not exist"));
        }

        let content_length = content_length.or(decoded_content_length).ok_or_else(|| {
            s3_error!(
                MissingContentLength,
                "You did not provide the number of bytes in the Content-Length HTTP header."
            )
        })?;
        let len = usize::try_from(content_length)
            .map_err(|_| s3_error!(InvalidRequest, "Invalid Content-Length HTTP header."))?;

        // if the content length is less than the max inlined data length, we store the object in the
        // metadata store, otherwise we store it in the cas layer.
        {
            use futures::TryStreamExt;
            if len <= self.casfs.max_inlined_data_length() {
                // Collect stream into Vec<u8>
                // it is safe to collect the stream into memory as the content length is
                // considered small
                let data: Vec<u8> = body
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(|e| s3_error!(InternalError, "Failed to read body: {}", e))?
                    .into_iter()
                    .flatten()
                    .collect();
                if let Some(expected) = expected_md5
                    && Md5::digest(&data).as_slice() != expected
                {
                    return Err(bad_digest());
                }
                let obj_meta = try_!(self.casfs.store_inlined_object(&bucket, &key, data).await);

                let output = PutObjectOutput {
                    e_tag: Some(ETag::Strong(obj_meta.format_e_tag())),
                    ..Default::default()
                };
                return Ok(S3Response::new(output));
            }
        }

        // save the datadata
        let converted_stream = convert_stream_error(body);
        let byte_stream = AsyncByteStream::new(converted_stream);
        let obj_meta = try_!(
            self.casfs
                .store_single_object_and_meta(&bucket, &key, byte_stream, len)
                .await
        );

        // The body is hashed while it is being stored, so a Content-MD5
        // mismatch is only known after the write; roll the object back
        // before failing the request.
        if let Some(expected) = expected_md5
            && obj_meta.hash().as_slice() != expected
        {
            try_!(self.casfs.delete_object(&bucket, &key).await);
            return Err(bad_digest());
        }

        let output = PutObjectOutput {
            e_tag: Some(ETag::Strong(obj_meta.format_e_tag())),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    pub(super) async fn head_object_op(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let HeadObjectInput { bucket, key, .. } = req.input;

        if !try_!(self.casfs.bucket_exists(&bucket)) {
            return Err(s3_error!(NoSuchBucket, "Bucket does not exist"));
        }

        let obj_meta = match self.casfs.get_object_meta(&bucket, &key) {
            Ok(Some(obj_meta)) => obj_meta,
            Ok(None) => {
                return Err(s3_error!(NoSuchKey, "Object does not exist"));
            }
            Err(e) => {
                error!("Could not get object metadata: {}", e);
                return Err(s3_error!(ServiceUnavailable, "service unavailable"));
            }
        };

        let output = HeadObjectOutput {
            content_length: Some(obj_meta.size() as i64),
            //content_type: Some(content_type),
            last_modified: Some(obj_meta.last_modified().into()),
            e_tag: Some(ETag::Strong(obj_meta.format_e_tag())),
            accept_ranges: Some("bytes".to_string()),
            //metadata: object_metadata,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    pub(super) async fn delete_object_op(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        info!("DELETE OBJECT: {:?}", req.input);

        let DeleteObjectInput { bucket, key, .. } = req.input;

        if !try_!(self.casfs.key_exists(&bucket, &key)) {
            return Err(s3_error!(NoSuchKey, "Key does not exist"));
        }

        // TODO: check for the key existence?
        try_!(self.casfs.delete_object(&bucket, &key).await);

        let output = DeleteObjectOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    pub(super) async fn delete_objects_op(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        info!("DELETE OBJECTS: {:?}", req.input);

        let DeleteObjectsInput { bucket, delete, .. } = req.input;

        if !try_!(self.casfs.bucket_exists(&bucket)) {
            return Err(s3_error!(NoSuchBucket, "Bucket does not exist"));
        }

        let mut deleted_objects = Vec::with_capacity(delete.objects.len());
        let errors = Vec::new();

        for object in delete.objects {
            match self.casfs.delete_object(&bucket, &object.key).await {
                Ok(_) => {
                    deleted_objects.push(DeletedObject {
                        key: Some(object.key),
                        ..DeletedObject::default()
                    });
                }
                Err(e) => {
                    error!(
                        "Could not remove key {} from bucket {}, error: {}",
                        &object.key, &bucket, e
                    );
                    // TODO
                    // errors.push(code_error!(InternalError, "Could not delete key"));
                }
            };
        }

        let output = DeleteObjectsOutput {
            deleted: Some(deleted_objects),
            errors: if errors.is_empty() {
                None
            } else {
                Some(errors)
            },
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    pub(super) async fn copy_object_op(
        &self,
        _: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        // see https://github.com/threefoldtech/s3-cas/blob/bee016998a4167b781082e1897072af0a64992e2/src/cas/fs.rs#L522-L560
        // which i'm not sure if it's implemented correctly
        Err(s3_error!(NotImplemented))
    }
}
