use s3s::S3Result;
use s3s::dto::{
    ETag, ListObjectsInput, ListObjectsOutput, ListObjectsV2Input, ListObjectsV2Output,
};
use s3s::s3_error;
use s3s::{S3Request, S3Response};
use tracing::{error, info};

use faster_hex::{hex_decode, hex_string};

use super::MAX_KEYS;
use super::S3Cas;

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

impl S3Cas {
    pub(super) async fn list_objects_op(
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
    pub(super) async fn list_objects_v2_op(
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
}
