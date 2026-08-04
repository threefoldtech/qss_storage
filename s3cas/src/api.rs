use std::io;

use bytes::Bytes;
use faster_hex::{hex_decode, hex_string};
use futures::Stream;
use futures::StreamExt;
use md5::{Digest, Md5};
use tracing::error;
use tracing::info;
use uuid::Uuid;

use cas_storage::AsyncByteStream;
use s3s::S3;
use s3s::S3Result;
use s3s::dto::StreamingBlob;
use s3s::dto::Timestamp;
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, CompleteMultipartUploadInput,
    CompleteMultipartUploadOutput, CopyObjectInput, CopyObjectOutput, CreateBucketInput,
    CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput, DeleteBucketInput,
    DeleteBucketOutput, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput,
    DeleteObjectsOutput, DeletedObject, ETag, GetBucketLocationInput, GetBucketLocationOutput,
    GetObjectInput, GetObjectOutput, HeadBucketInput, HeadBucketOutput, HeadObjectInput,
    HeadObjectOutput, ListBucketsInput, ListBucketsOutput, ListMultipartUploadsInput,
    ListMultipartUploadsOutput, ListObjectsInput, ListObjectsOutput, ListObjectsV2Input,
    ListObjectsV2Output, ListPartsInput, ListPartsOutput, MultipartUpload, Part, PutObjectInput,
    PutObjectOutput, UploadPartInput, UploadPartOutput,
};
use s3s::s3_error;
use s3s::{S3Request, S3Response};

use crate::metrics::SharedMetrics;
use cas_storage::cas::UploadClaim;
use cas_storage::metastore::UploadRecord;
use cas_storage::{BlockId, ContentHash, MultiPart, ObjectData};
use cas_storage::{BlockStream, CasFS};

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

use cas_storage::RangeRequest;
impl S3Cas {
    pub fn new(casfs: CasFS, metrics: SharedMetrics) -> Self {
        match casfs.list_buckets() {
            Ok(buckets) => metrics.set_bucket_count(buckets.len()),
            Err(e) => error!("Could not count buckets for metrics: {}", e),
        }

        Self { casfs, metrics }
    }
}

/// Compute the content hash and total size of a completed multipart upload.
///
/// Per the S3 convention the ETag of a multipart object is the MD5 of the
/// concatenated MD5 digests of the parts -- not of the object bytes, and not
/// of the block addresses. Each part's digest is already stored in its
/// `MultiPart` record by `upload_part`. The "-{part count}" suffix that
/// completes the ETag is added by `Object::format_e_tag`.
fn calculate_multipart_hash(parts: &[MultiPart]) -> (ContentHash, u64) {
    let mut hasher = Md5::new();
    let mut size: u64 = 0;

    for part in parts {
        hasher.update(part.hash().as_slice());
        size += part.size() as u64;
    }

    (ContentHash(hasher.finalize().into()), size)
}

fn fmt_content_range(start: u64, end_inclusive: u64, size: u64) -> String {
    format!("bytes {start}-{end_inclusive}/{size}")
}

/// One stored object as the listing wire shape.
fn object_dto(key: String, obj: &cas_storage::Object) -> s3s::dto::Object {
    s3s::dto::Object {
        key: Some(key),
        e_tag: Some(ETag::Strong(obj.format_e_tag())),
        last_modified: Some(obj.last_modified().into()),
        owner: None,
        size: Some(obj.size() as i64),
        storage_class: None,
        ..Default::default()
    }
}

/// One page of a delimiter listing: objects and rolled-up common prefixes
/// taken from one lexicographic key stream.
struct DelimitedPage {
    objects: Vec<s3s::dto::Object>,
    common_prefixes: Vec<String>,
    truncated: bool,
    /// The last underlying key this page consumed -- delivered as an
    /// object, or swallowed into a rolled-up prefix. The next page resumes
    /// strictly after it, which is what keeps a common prefix from being
    /// listed again by the page that follows.
    resume_after: Option<String>,
}

/// Rolls a sorted key stream into one delimiter page, the way S3 defines
/// it: a key whose remainder after `prefix` contains `delimiter` is rolled
/// up into the prefix that ends at the delimiter's first occurrence; each
/// distinct roll-up counts once against the page size, like an object.
///
/// A page never ends inside a group. Once a prefix is rolled up, every
/// following key under it is consumed before the page can close --
/// otherwise the resume point would land inside the group and the next
/// page would repeat the prefix. That consumption is a linear walk over
/// the group's keys; a seek would need a start-at (not start-after) lower
/// bound the metastore iterator does not offer today.
fn collect_delimited_page(
    iter: impl Iterator<Item = (String, cas_storage::Object)>,
    prefix: &str,
    delimiter: &str,
    page_size: usize,
) -> DelimitedPage {
    let mut page = DelimitedPage {
        objects: Vec::new(),
        common_prefixes: Vec::new(),
        truncated: false,
        resume_after: None,
    };
    let mut items = 0usize;
    let mut group: Option<String> = None;

    for (key, obj) in iter {
        if let Some(g) = &group
            && key.starts_with(g.as_str())
        {
            // Inside the group the page already rolled up: consumed, not
            // delivered.
            page.resume_after = Some(key);
            continue;
        }
        if items == page_size {
            // A fresh item past the page is the probe that proves
            // truncation; it is not consumed, so the resume point stays on
            // this page's last item.
            page.truncated = true;
            break;
        }
        let rest = key.strip_prefix(prefix).unwrap_or(key.as_str());
        match rest.find(delimiter) {
            Some(pos) => {
                let rolled = format!("{prefix}{}", &rest[..pos + delimiter.len()]);
                page.common_prefixes.push(rolled.clone());
                group = Some(rolled);
            }
            None => {
                group = None;
                page.objects.push(object_dto(key.clone(), &obj));
            }
        }
        page.resume_after = Some(key);
        items += 1;
    }
    page
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

/// The answer to every operation that could not find (or could not claim) an
/// upload record: an unknown id, and the loser of a complete-versus-abort
/// race alike (ADR 0003 -- the record is the only linearization point, so
/// "somebody else got there first" and "it never existed" are one outcome).
fn no_such_upload() -> s3s::S3Error {
    s3_error!(
        NoSuchUpload,
        "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed."
    )
}

/// Unix seconds as stored in an upload record, as a wire timestamp. Pre-epoch
/// values are impossible in a record this side of a broken clock, and clamp to
/// the epoch rather than panicking.
fn timestamp_from_secs(secs: i64) -> Timestamp {
    let secs = u64::try_from(secs).unwrap_or(0);
    Timestamp::from(std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs))
}

/// One part record as the `ListParts` wire shape.
///
/// The ETag is the part's MD5 in hex -- byte for byte what `upload_part`
/// answered, which is what clients echo back in `CompleteMultipartUpload`.
/// `last_modified` is `None`: part records carry no timestamp of their own,
/// and inventing one would be worse than omitting an optional field.
fn part_to_dto(part: &MultiPart) -> Part {
    Part {
        part_number: Some(part.part_number() as i32),
        size: Some(part.size() as i64),
        e_tag: Some(ETag::Strong(part.hash().to_hex())),
        last_modified: None,
        ..Default::default()
    }
}

/// One upload record as the `ListMultipartUploads` wire shape.
fn upload_to_dto(record: &UploadRecord) -> MultipartUpload {
    MultipartUpload {
        key: Some(record.key().to_string()),
        upload_id: Some(record.upload_id().to_string()),
        initiated: Some(timestamp_from_secs(record.created_at())),
        ..Default::default()
    }
}

#[async_trait::async_trait]
impl S3 for S3Cas {
    /// Claims the upload and reaps every part it holds; the loser of the
    /// claim -- and an id that never existed -- gets `NoSuchUpload` (ADR 0003
    /// decision, owner sign-off). The reaping itself (record first, blocks
    /// second: hard rule 2) lives in `CasFS::abort_upload`, which the
    /// stale-upload GC calls too.
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let AbortMultipartUploadInput {
            bucket,
            key,
            upload_id,
            ..
        } = req.input;

        let Some(parts) = try_!(self.casfs.abort_upload(&bucket, &key, &upload_id).await) else {
            return Err(no_such_upload());
        };

        info!(
            "ABORT MULTIPART UPLOAD: bucket={bucket} key={key} upload_id={upload_id} parts={parts}"
        );
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let CompleteMultipartUploadInput {
            multipart_upload,
            bucket,
            key,
            upload_id,
            ..
        } = req.input;

        let multipart_upload = if let Some(multipart_upload) = multipart_upload {
            multipart_upload
        } else {
            let err = s3_error!(InvalidPart, "Missing multipart_upload");
            return Err(err);
        };

        // An empty parts list used to sail through and mint an empty object
        // (ADR 0003 decision 3, owner sign-off): a complete with no parts is
        // a malformed request, not a zero-byte upload.
        if multipart_upload.parts.iter().flatten().next().is_none() {
            return Err(s3_error!(
                InvalidRequest,
                "You must specify at least one part"
            ));
        }

        // Existence at entry, the point read (ADR 0003 decision 3 names both
        // this and the claim below). Without it, a complete arriving after
        // the upload was aborted would fail the part validation instead --
        // InvalidPart for an upload that is simply gone. The claim remains
        // the authoritative answer; this only keeps the common case honest.
        if try_!(self.casfs.get_upload(&bucket, &key, &upload_id)).is_none() {
            return Err(no_such_upload());
        }

        // Validation runs against the still-unclaimed upload, so a request
        // this rejects leaves the upload intact and the client can retry a
        // corrected complete -- what S3 does on an InvalidPart. It collects
        // the part NUMBERS; the claim below re-reads the records themselves,
        // and those values are the authoritative ones.
        let mut part_numbers = vec![];
        let mut cnt: i32 = 0;
        for part in multipart_upload.parts.iter().flatten() {
            // validate part number
            let part_number =
                try_!(part.part_number.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "Missing part_number")
                }));
            cnt = cnt.wrapping_add(1);
            if part_number != cnt {
                try_!(Err(io::Error::other("InvalidPartOrder")));
            }

            let result =
                self.casfs
                    .get_multipart_part(&bucket, &key, &upload_id, part_number as i64);
            match result {
                Ok(Some(_)) => {}
                Ok(None) => {
                    error!(
                        "Missing part \"{}\" in multipart upload: part not found",
                        part_number
                    );
                    return Err(s3_error!(InvalidArgument, "Part not uploaded"));
                }
                Err(e) => {
                    error!(
                        "Missing part \"{}\" in multipart upload: {}",
                        part_number, e
                    );
                    return Err(s3_error!(InvalidArgument, "Part not uploaded"));
                }
            };
            part_numbers.push(part_number as i64);
        }

        // The claim: one transaction that takes the upload record AND every
        // part record named above. It is the only thing serializing this
        // against a concurrent abort or a second complete -- whoever takes the
        // upload record proceeds, everyone else answers NoSuchUpload.
        //
        // The parts are IN the claim because this object inherits their block
        // references instead of taking new ones. If the part records outlived
        // the upload record even for an instant, the GC's orphan sweep could
        // see them for what they are not -- parts no upload owns -- and
        // release references this object now holds: loss. Both trees live in
        // the shared DB, so one transaction closes that window rather than
        // narrowing it (ADR 0003 amendment).
        //
        // Validating first is safe because while the upload record exists,
        // part records are removed only by complete, abort and the GC, and all
        // three must win a claim before they touch one. What a concurrent
        // caller CAN do is re-upload a part between validation and claim, in
        // which case the claim reads the NEWER record and the object is built
        // from it; the replaced record's blocks stay over-counted until the
        // next recount, the leak direction (ADR 0005), never loss.
        //
        // The claim happens BEFORE the object is created, so a crash in
        // between leaves blocks whose rc counts holders that no longer exist:
        // over-counts, which the recount collects. Bounded leakage; claiming
        // after the object existed would instead let a second complete mint
        // the object twice over.
        let parts = match try_!(self.casfs.claim_upload_with_parts(
            &bucket,
            &key,
            &upload_id,
            &part_numbers
        )) {
            UploadClaim::Claimed { parts, .. } => parts,
            UploadClaim::NoUpload => return Err(no_such_upload()),
            // A part validated a moment ago is gone: nothing was claimed, the
            // upload survives, and a corrected complete can still be sent --
            // the same answer the validation loop gives.
            UploadClaim::MissingPart(part_number) => {
                error!("Part \"{part_number}\" vanished between validation and claim");
                return Err(s3_error!(InvalidArgument, "Part not uploaded"));
            }
        };

        let (content_hash, size) = calculate_multipart_hash(&parts);
        let blocks: Vec<BlockId> = parts
            .iter()
            .flat_map(|mp| mp.blocks().iter().copied())
            .collect();

        // The overwrite case is a complete onto an existing key: the record it
        // displaces is a different object, and its references are released
        // after this commit (ADR 0008). The blocks THIS object inherits came
        // out of the claim above and are untouched by that release.
        let object_meta = try_!(
            self.casfs
                .create_object_meta(
                    &bucket,
                    &key,
                    size,
                    content_hash,
                    ObjectData::MultiPart {
                        blocks,
                        parts: parts.len()
                    },
                )
                .await
        );

        // No cleanup loop: the claim above already removed every part record
        // it returned, in the same transaction that removed the upload record.
        // Parts the client did NOT name were not claimed and are now orphans
        // -- their references were never inherited by this object, so the GC
        // reaps them correctly (ADR 0003 amendment).

        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            e_tag: Some(ETag::Strong(object_meta.format_e_tag())),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn copy_object(
        &self,
        _: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        // see https://github.com/threefoldtech/s3-cas/blob/bee016998a4167b781082e1897072af0a64992e2/src/cas/fs.rs#L522-L560
        // which i'm not sure if it's implemented correctly
        Err(s3_error!(NotImplemented))
    }

    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let input = req.input;

        info!("create bucket");
        if try_!(self.casfs.bucket_exists(&input.bucket)) {
            return Err(s3_error!(
                BucketAlreadyExists,
                "A bucket with this name already exists"
            ));
        }

        try_!(self.casfs.create_bucket(&input.bucket));

        self.metrics.inc_bucket_count();

        let output = CreateBucketOutput {
            location: Some(format!("/{}", input.bucket)),
        };
        Ok(S3Response::new(output))
    }

    /// Mints an upload id and records the upload.
    ///
    /// Writing that record is what brings the upload into existence (ADR
    /// 0003): before it lands `upload_part` refuses the id, and once it is
    /// claimed away the upload is over. It also stamps the creation time the
    /// stale-upload GC ages against, so an id is never handed out without one.
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let CreateMultipartUploadInput { bucket, key, .. } = req.input;

        if !try_!(self.casfs.bucket_exists(&bucket)) {
            return Err(s3_error!(NoSuchBucket, "Bucket does not exist"));
        }

        let upload_id = Uuid::new_v4().to_string();
        try_!(self.casfs.create_upload(&bucket, &key, &upload_id));

        let output = CreateMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id.to_string()),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let DeleteBucketInput { bucket, .. } = req.input;

        try_!(self.casfs.bucket_delete(&bucket).await);

        self.metrics.dec_bucket_count();

        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    async fn delete_object(
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

    async fn delete_objects(
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

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        let input = req.input;
        let exists = try_!(self.casfs.bucket_exists(&input.bucket));

        if !exists {
            return Err(s3_error!(NoSuchBucket));
        }

        let output = GetBucketLocationOutput::default();
        Ok(S3Response::new(output))
    }

    async fn get_object(
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

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let HeadBucketInput { bucket, .. } = req.input;

        if !try_!(self.casfs.bucket_exists(&bucket)) {
            return Err(s3_error!(
                NoSuchBucket,
                "The specified bucket does not exist"
            ));
        }

        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn head_object(
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

    async fn list_buckets(
        &self,
        _: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let csfs_buckets = try_!(self.casfs.list_buckets());
        let mut buckets = Vec::with_capacity(csfs_buckets.len());
        for bucket in csfs_buckets {
            let bucket = Bucket {
                creation_date: Some(Timestamp::from(bucket.ctime())),
                name: Some(bucket.name().into()),
                bucket_region: None,
            };
            buckets.push(bucket);
        }
        let output = ListBucketsOutput {
            buckets: Some(buckets),
            owner: None,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    /// The in-flight uploads of one bucket, in S3 order (key, then upload id).
    ///
    /// The whole `_UPLOADS` tree is decoded and then filtered, sorted and
    /// paginated in memory. That is the ADR 0003 trade: the set is bounded by
    /// the GC's TTL, and sorting here is what lets the tree key for point
    /// reads instead of for collation.
    ///
    /// `delimiter` is accepted and echoed but never groups: common-prefix
    /// grouping is deferred until a client needs it (ADR 0003 open question),
    /// and an empty `common_prefixes` is the conformant answer meanwhile.
    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let ListMultipartUploadsInput {
            bucket,
            delimiter,
            encoding_type,
            key_marker,
            max_uploads,
            prefix,
            upload_id_marker,
            ..
        } = req.input;

        if !try_!(self.casfs.bucket_exists(&bucket)) {
            return Err(s3_error!(NoSuchBucket, "Bucket does not exist"));
        }

        let mut uploads = try_!(self.casfs.list_uploads());
        uploads.retain(|record| {
            record.bucket() == bucket
                && prefix
                    .as_ref()
                    .is_none_or(|prefix| record.key().starts_with(prefix.as_str()))
        });
        uploads.sort_unstable_by(|a, b| {
            a.key()
                .cmp(b.key())
                .then_with(|| a.upload_id().cmp(b.upload_id()))
        });

        // Markers name the upload the previous page ended on, so listing
        // resumes strictly after it. An upload_id_marker alone is meaningless
        // (it only disambiguates within one key) and S3 ignores it; without
        // one, the whole marked key is behind us.
        if let Some(key_marker) = key_marker.as_deref() {
            let upload_id_marker = upload_id_marker.as_deref();
            uploads.retain(|record| match record.key().cmp(key_marker) {
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Less => false,
                std::cmp::Ordering::Equal => {
                    upload_id_marker.is_some_and(|marker| record.upload_id() > marker)
                }
            });
        }

        let max_uploads = max_uploads.unwrap_or(MAX_UPLOADS).clamp(0, MAX_UPLOADS);
        // One past the page: its presence IS the truncation flag.
        let truncated = uploads.len() > max_uploads as usize;
        uploads.truncate(max_uploads as usize);
        let (next_key_marker, next_upload_id_marker) = match uploads.last() {
            Some(last) if truncated => (
                Some(last.key().to_string()),
                Some(last.upload_id().to_string()),
            ),
            _ => (None, None),
        };

        let output = ListMultipartUploadsOutput {
            bucket: Some(bucket),
            uploads: Some(uploads.iter().map(upload_to_dto).collect()),
            delimiter,
            encoding_type,
            prefix,
            key_marker,
            upload_id_marker,
            next_key_marker,
            next_upload_id_marker,
            max_uploads: Some(max_uploads),
            is_truncated: Some(truncated),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let ListObjectsInput {
            bucket,
            delimiter,
            prefix,
            encoding_type,
            marker,
            max_keys,
            ..
        } = req.input;

        let key_count = max_keys.unwrap_or(MAX_KEYS).clamp(0, MAX_KEYS);

        // The existence check must come before get_bucket: the store opens
        // trees with create-if-missing semantics, so listing an absent
        // bucket would quietly will it into being instead of failing.
        if !try_!(self.casfs.bucket_exists(&bucket)) {
            return Err(s3_error!(NoSuchBucket, "Bucket does not exist"));
        }

        let b = try_!(self.casfs.get_bucket(&bucket));

        let (objects, common_prefixes, truncated, next_marker) =
            if let Some(delim) = delimiter.as_deref().filter(|d| !d.is_empty()) {
                let page = collect_delimited_page(
                    b.range_filter(marker.clone(), prefix.clone(), None),
                    prefix.as_deref().unwrap_or(""),
                    delim,
                    key_count as usize,
                );
                let marker = if page.truncated {
                    page.resume_after
                } else {
                    None
                };
                (page.objects, page.common_prefixes, page.truncated, marker)
            } else {
                let mut objects = b
                    .range_filter(marker.clone(), prefix.clone(), None)
                    .map(|(key, obj)| object_dto(key, &obj))
                    .take(key_count as usize + 1)
                    .collect::<Vec<_>>();

                let truncated = objects.len() > key_count as usize;
                if truncated {
                    // Drop the probe row and name the last key DELIVERED as
                    // the marker: range_filter resumes strictly after its
                    // marker, so handing out the probe key itself would
                    // skip that key.
                    objects.pop();
                }
                let next_marker = if truncated {
                    objects.last().and_then(|o| o.key.clone())
                } else {
                    None
                };
                (objects, Vec::new(), truncated, next_marker)
            };

        // NextMarker is answered for a delimiter listing (AWS's own
        // contract: without one, clients derive the marker from the last
        // Contents key, which a page ending on a rolled-up prefix does not
        // have) -- and otherwise only for a request that itself paginated.
        let answer_marker = delimiter.is_some() || marker.is_some();
        let output = ListObjectsOutput {
            contents: Some(objects),
            common_prefixes: (!common_prefixes.is_empty()).then(|| {
                common_prefixes
                    .into_iter()
                    .map(|prefix| s3s::dto::CommonPrefix {
                        prefix: Some(prefix),
                    })
                    .collect()
            }),
            delimiter,
            encoding_type,
            name: Some(bucket),
            is_truncated: Some(truncated),
            next_marker: if answer_marker { next_marker } else { None },
            marker,
            max_keys: Some(key_count),
            prefix,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    /// <p>StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts listing after this
    /// specified key. StartAfter can be any key in the bucket.</p>
    ///
    /// <code>ContinuationToken</code> indicates to Amazon S3 that the list is being continued on
    /// this bucket with a token. <code>ContinuationToken</code> is obfuscated and is not a real
    /// key. You can use this <code>ContinuationToken</code> for pagination of the list results.  </p>
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        info!("LIST OBJECTS V2: {:?}", req.input);
        let ListObjectsV2Input {
            bucket,
            delimiter,
            prefix,
            encoding_type,
            start_after,
            max_keys,
            continuation_token,
            ..
        } = req.input;

        // Same order as list_objects: existence first, because get_bucket
        // creates what it cannot find.
        if !try_!(self.casfs.bucket_exists(&bucket)) {
            return Err(s3_error!(NoSuchBucket, "Bucket does not exist"));
        }

        let b = try_!(self.casfs.get_bucket(&bucket));

        // max number of keys to return, default is MAX_KEYS(1000)
        let key_count = max_keys.unwrap_or(MAX_KEYS).clamp(0, MAX_KEYS);

        // continuation token
        let decoded_continuation_token = decode_continuation_token(continuation_token.as_deref())?;

        // One probe row past the page decides is_truncated: a full page
        // with nothing behind it must not claim truncation, or the client
        // is sent on one more round trip for an empty page.
        //
        // The token is the last key a page CONSUMED: range_filter resumes
        // strictly after it. The paginator itself never reads it -- it
        // keys on is_truncated, whose omission was the bug that made
        // every listing stop at one page.
        let (objects, common_prefixes, truncated, next_token) =
            if let Some(delim) = delimiter.as_deref().filter(|d| !d.is_empty()) {
                let page = collect_delimited_page(
                    b.range_filter(
                        start_after.clone(),
                        prefix.clone(),
                        decoded_continuation_token,
                    ),
                    prefix.as_deref().unwrap_or(""),
                    delim,
                    key_count as usize,
                );
                let token = if page.truncated {
                    page.resume_after.map(|key| hex_string(key.as_bytes()))
                } else {
                    None
                };
                (page.objects, page.common_prefixes, page.truncated, token)
            } else {
                let mut objects: Vec<_> = b
                    .range_filter(
                        start_after.clone(),
                        prefix.clone(),
                        decoded_continuation_token,
                    )
                    .map(|(key, obj)| object_dto(key, &obj))
                    .take(key_count as usize + 1)
                    .collect();

                let truncated = objects.len() > key_count as usize;
                if truncated {
                    objects.pop();
                }
                let next_token = match (truncated, objects.last()) {
                    (true, Some(last)) => last.key.as_ref().map(|key| hex_string(key.as_bytes())),
                    _ => None,
                };
                (objects, Vec::new(), truncated, next_token)
            };

        // KeyCount counts what this page answers: objects AND rolled-up
        // prefixes, which is what MaxKeys bounded.
        let output = ListObjectsV2Output {
            key_count: Some((objects.len() + common_prefixes.len()) as i32),
            max_keys: Some(key_count),
            is_truncated: Some(truncated),
            contents: Some(objects),
            common_prefixes: (!common_prefixes.is_empty()).then(|| {
                common_prefixes
                    .into_iter()
                    .map(|prefix| s3s::dto::CommonPrefix {
                        prefix: Some(prefix),
                    })
                    .collect()
            }),
            continuation_token,
            delimiter,
            encoding_type,
            name: Some(bucket),
            prefix,
            start_after,
            next_continuation_token: next_token,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    /// The parts of one upload, ascending by part number.
    ///
    /// Read-only, and bounded by S3's 10k parts per upload, so the prefix scan
    /// is decoded and paginated in memory (ADR 0003 decision 7). An upload
    /// with no record answers `NoSuchUpload`; an upload with no parts yet
    /// answers an empty list, which is a different thing.
    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let ListPartsInput {
            bucket,
            key,
            max_parts,
            part_number_marker,
            upload_id,
            ..
        } = req.input;

        if try_!(self.casfs.get_upload(&bucket, &key, &upload_id)).is_none() {
            return Err(no_such_upload());
        }

        let mut parts = try_!(self.casfs.upload_parts(&bucket, &key, &upload_id));
        // The scan already yields part_number order, but the sort is what the
        // ADR promises the client: ordering comes from here, not from the
        // store's key layout.
        parts.sort_unstable_by_key(cas_storage::MultiPart::part_number);

        // The marker names the last part of the previous page; listing
        // resumes strictly after it.
        if let Some(marker) = part_number_marker {
            parts.retain(|part| part.part_number() > i64::from(marker));
        }

        let max_parts = max_parts.unwrap_or(MAX_PARTS).clamp(0, MAX_PARTS);
        let truncated = parts.len() > max_parts as usize;
        parts.truncate(max_parts as usize);
        let next_part_number_marker = match parts.last() {
            Some(last) if truncated => Some(last.part_number() as i32),
            _ => None,
        };

        let output = ListPartsOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id),
            parts: Some(parts.iter().map(part_to_dto).collect()),
            part_number_marker,
            next_part_number_marker,
            max_parts: Some(max_parts),
            is_truncated: Some(truncated),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn put_object(
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

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let decoded_content_length = decoded_content_length(&req.headers);
        let UploadPartInput {
            body,
            bucket,
            content_length,
            content_md5,
            key,
            part_number,
            upload_id,
            ..
        } = req.input;

        // At entry, before a single block is streamed: an unknown upload id
        // used to be accepted, writing part records and refcounts for an
        // upload nothing would ever complete or abort (ADR 0003 decision 3).
        //
        // Deliberately a plain read, not a claim: this must not exclude the
        // complete or abort of the upload it is checking. A part that passes
        // here and lands after another caller claimed the record becomes an
        // orphan part -- refcounted blocks with no upload -- which the GC
        // reaps. Bounded leakage, no loss.
        if try_!(self.casfs.get_upload(&bucket, &key, &upload_id)).is_none() {
            return Err(no_such_upload());
        }

        let expected_md5 = content_md5.as_deref().map(parse_content_md5).transpose()?;

        let Some(body) = body else {
            return Err(s3_error!(IncompleteBody));
        };

        let content_length = content_length.or(decoded_content_length).ok_or_else(|| {
            s3_error!(
                MissingContentLength,
                "You did not provide the number of bytes in the Content-Length HTTP header."
            )
        })?;

        let converted_stream = convert_stream_error(body);
        let byte_stream = AsyncByteStream::new(converted_stream);

        // we only store the object here, metadata is not stored in the meta store.
        // it is stored in the multipart metadata, in the `cas` layer.
        // the multipart metadata will be deleted when the multipart upload is completed
        // and replaced with the object metadata in metastore in the `complete_multipart_upload` function.
        let (blocks, hash, size) = try_!(self.casfs.store_object(&bucket, &key, byte_stream).await);

        if u64::try_from(content_length) != Ok(size) {
            return Err(s3_error!(
                InvalidRequest,
                "You did not send the amount of bytes specified by the Content-Length HTTP header."
            ));
        }

        // The part was just stored, so its size fits the address space.
        let part_size = usize::try_from(size)
            .map_err(|_| s3_error!(InternalError, "part size exceeds address space"))?;

        // Like the length check above, this fails the part before it is
        // registered; the blocks already written stay behind, addressed by
        // content, and are reused if the part is retried with the same data.
        if let Some(expected) = expected_md5
            && hash.as_slice() != expected
        {
            return Err(bad_digest());
        }

        try_!(self.casfs.insert_multipart_part(
            bucket,
            key,
            part_size,
            part_number as i64,
            upload_id,
            hash,
            blocks
        ));

        let e_tag = ETag::Strong(hash.to_hex());

        let output = UploadPartOutput {
            e_tag: Some(e_tag),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }
}

// Add helper function
fn convert_stream_error(body: StreamingBlob) -> impl Stream<Item = Result<Bytes, io::Error>> {
    body.map(|r| r.map_err(|e| io::Error::other(e.to_string())))
}

fn decode_continuation_token(rt: Option<&str>) -> Result<Option<String>, s3s::S3Error> {
    if let Some(rt) = rt {
        let mut out = vec![0; rt.len() / 2];
        if hex_decode(rt.as_bytes(), &mut out).is_err() {
            return Err(s3_error!(
                InvalidToken,
                "continuation token has an invalid format"
            ));
        };

        String::from_utf8(out)
            .map(Some)
            .map_err(|_| s3_error!(InvalidToken, "continuation token is invalid"))
    } else {
        Ok(None)
    }
}

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
