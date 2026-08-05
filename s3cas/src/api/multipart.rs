use std::io;

use md5::{Digest, Md5};
use uuid::Uuid;

use s3s::S3Result;
use s3s::dto::Timestamp;
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, CompleteMultipartUploadInput,
    CompleteMultipartUploadOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput, ETag,
    ListMultipartUploadsInput, ListMultipartUploadsOutput, ListPartsInput, ListPartsOutput,
    MultipartUpload, Part, UploadPartInput, UploadPartOutput,
};
use s3s::s3_error;
use s3s::{S3Request, S3Response};
use tracing::{error, info};

use cas_storage::AsyncByteStream;
use cas_storage::cas::UploadClaim;
use cas_storage::metastore::UploadRecord;
use cas_storage::{BlockId, ContentHash, MultiPart, ObjectData};

use super::S3Cas;
use super::{
    MAX_PARTS, MAX_UPLOADS, bad_digest, convert_stream_error, decoded_content_length,
    parse_content_md5,
};

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

impl S3Cas {
    /// Claims the upload and reaps every part it holds; the loser of the
    /// claim -- and an id that never existed -- gets `NoSuchUpload` (ADR 0003
    /// decision, owner sign-off). The reaping itself (record first, blocks
    /// second: hard rule 2) lives in `CasFS::abort_upload`, which the
    /// stale-upload GC calls too.
    pub(super) async fn abort_multipart_upload_op(
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

    pub(super) async fn complete_multipart_upload_op(
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

    /// Mints an upload id and records the upload.
    ///
    /// Writing that record is what brings the upload into existence (ADR
    /// 0003): before it lands `upload_part` refuses the id, and once it is
    /// claimed away the upload is over. It also stamps the creation time the
    /// stale-upload GC ages against, so an id is never handed out without one.
    pub(super) async fn create_multipart_upload_op(
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
    pub(super) async fn list_multipart_uploads_op(
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

    /// The parts of one upload, ascending by part number.
    ///
    /// Read-only, and bounded by S3's 10k parts per upload, so the prefix scan
    /// is decoded and paginated in memory (ADR 0003 decision 7). An upload
    /// with no record answers `NoSuchUpload`; an upload with no parts yet
    /// answers an empty list, which is a different thing.
    pub(super) async fn list_parts_op(
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

    pub(super) async fn upload_part_op(
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
