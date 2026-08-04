use crate::common::{METADATA_DBS, create_bucket, error_code, serial, setup_test};

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;

use anyhow::Result;
use s3cas::cas::StorageEngine;
use tracing::{debug, error};
use uuid::Uuid;

#[tokio::test]
#[tracing::instrument]
async fn test_list_bucket() -> Result<()> {
    for engine in METADATA_DBS {
        do_test_list_buckets(engine).await?;
    }
    Ok(())
}

async fn do_test_list_buckets(engine: s3cas::cas::StorageEngine) -> Result<()> {
    let c = Client::new(setup_test(engine, Some(1)));
    let response1 = log_and_unwrap!(c.list_buckets().send().await);
    drop(response1);

    let bucket1 = format!("test-list-buckets-1-{}", Uuid::new_v4());
    let bucket1_str = bucket1.as_str();
    let bucket2 = format!("test-list-buckets-2-{}", Uuid::new_v4());
    let bucket2_str = bucket2.as_str();

    create_bucket(&c, bucket1_str).await?;
    create_bucket(&c, bucket2_str).await?;

    let response2 = log_and_unwrap!(c.list_buckets().send().await);
    let bucket_names: Vec<_> = response2
        .buckets()
        .iter()
        .filter_map(|bucket| bucket.name())
        .collect();
    assert!(bucket_names.contains(&bucket1_str));
    assert!(bucket_names.contains(&bucket2_str));

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_list_objects_v2() -> Result<()> {
    for engine in METADATA_DBS {
        do_test_list_objects_v2(engine).await?;
    }
    Ok(())
}

async fn do_test_list_objects_v2(engine: s3cas::cas::StorageEngine) -> Result<()> {
    let c = Client::new(setup_test(engine, Some(1)));
    let bucket = format!("test-list-objects-v2-{}", Uuid::new_v4());
    let bucket_str = bucket.as_str();
    create_bucket(&c, bucket_str).await?;

    let test_prefix = "this/is/a/test/";
    let key1 = "this/is/a/test/path/file1.txt";
    let key2 = "this/is/a/test/path/file2.txt";
    {
        let content = "hello world\nनमस्ते दुनिया\n";
        //let crc32c = base64_simd::STANDARD
        //    .encode_to_string(crc32c::crc32c(content.as_bytes()).to_be_bytes());
        c.put_object()
            .bucket(bucket_str)
            .key(key1)
            .body(ByteStream::from_static(content.as_bytes()))
            //.checksum_crc32_c(crc32c.as_str())
            .send()
            .await?;
        c.put_object()
            .bucket(bucket_str)
            .key(key2)
            .body(ByteStream::from_static(content.as_bytes()))
            //.checksum_crc32_c(crc32c.as_str())
            .send()
            .await?;
    }

    {
        // list objects v1
        let result = c
            .list_objects()
            .bucket(bucket_str)
            .prefix(test_prefix)
            .send()
            .await;

        let response = log_and_unwrap!(result);

        let contents: Vec<_> = response
            .contents()
            .iter()
            .filter_map(|obj| obj.key())
            .collect();
        assert!(!contents.is_empty());
        assert!(contents.contains(&key1));
        assert!(contents.contains(&key2));
    }

    {
        // list objects v2
        let result = c
            .list_objects_v2()
            .bucket(bucket_str)
            .prefix(test_prefix)
            .send()
            .await;

        let response = log_and_unwrap!(result);

        let contents: Vec<_> = response
            .contents()
            .iter()
            .filter_map(|obj| obj.key())
            .collect();
        assert!(!contents.is_empty());
        assert!(contents.contains(&key1));
        assert!(contents.contains(&key2));
    }

    Ok(())
}

#[tokio::test]
async fn test_list_objects_v2_startafter() -> Result<()> {
    for engine in METADATA_DBS {
        do_test_list_objects_v2_startafter(engine).await?;
    }
    Ok(())
}

async fn do_test_list_objects_v2_startafter(engine: StorageEngine) -> Result<()> {
    //env_logger::init_from_env(
    //    env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "error"),
    //);

    log::error!("Starting test_list_objects_v2_startafter");

    let c = Client::new(setup_test(engine, Some(1)));
    let bucket = format!("test-list-{}", Uuid::new_v4());
    let bucket_str = bucket.as_str();
    create_bucket(&c, bucket_str).await?;

    let test_prefix = "this/is/a/test/";
    let content = "hello world\n";
    let keys: Vec<String> = (1..=1100)
        .map(|i| format!("this/is/a/test/path/file{:04}.txt", i))
        .collect();
    {
        // create 1100 objects
        for key in keys {
            //log::error!("Creating object: bucket:{} key:{}", bucket_str, key);
            c.put_object()
                .bucket(bucket_str)
                .key(key)
                .body(ByteStream::from_static(content.as_bytes()))
                //.checksum_crc32_c(crc32c.as_str())
                .send()
                .await?;
        }
    }

    {
        // ------- without start_after & token
        let result = c
            .list_objects_v2()
            .bucket(bucket_str)
            .prefix(test_prefix)
            .send()
            .await;

        let response = log_and_unwrap!(result);

        let contents: Vec<_> = response
            .contents()
            .iter()
            .filter_map(|obj| obj.key())
            .collect();
        assert_eq!(contents.len(), 1000);
        assert_eq!(
            "this/is/a/test/path/file0001.txt",
            *contents.first().unwrap()
        );
        assert_eq!(
            "this/is/a/test/path/file1000.txt",
            *contents.last().unwrap()
        );

        assert!(response.next_continuation_token().is_some());
        // The CLI paginator keys on IsTruncated, not on the token: a page
        // with more behind it must say so, and key_count counts this page.
        assert_eq!(response.is_truncated(), Some(true));
        assert_eq!(response.key_count(), Some(1000));

        {
            // ------ next page using token

            let token = response.next_continuation_token().unwrap();
            let result = c
                .list_objects_v2()
                .bucket(bucket_str)
                .prefix(test_prefix)
                .continuation_token(token)
                .send()
                .await;

            let response = log_and_unwrap!(result);
            let contents: Vec<_> = response
                .contents()
                .iter()
                .filter_map(|obj| obj.key())
                .collect();
            assert_eq!(contents.len(), 100);

            assert_eq!(response.continuation_token().unwrap(), token);
            assert!(response.next_continuation_token().is_none());
            assert_eq!(response.is_truncated(), Some(false));
            assert_eq!(response.key_count(), Some(100));
            assert!(response.start_after().is_none());
            assert_eq!(
                "this/is/a/test/path/file1001.txt",
                *contents.first().unwrap()
            );
        }

        {
            // next page using start_after should give the same result
            let result = c
                .list_objects_v2()
                .bucket(bucket_str)
                .prefix(test_prefix)
                .start_after("this/is/a/test/path/file1000.txt")
                .send()
                .await;

            let response = log_and_unwrap!(result);
            let contents: Vec<_> = response
                .contents()
                .iter()
                .filter_map(|obj| obj.key())
                .collect();
            assert_eq!(contents.len(), 100);

            assert!(response.next_continuation_token().is_none());
            assert!(response.continuation_token().is_none());
            assert_eq!(response.is_truncated(), Some(false));
            assert!(response.start_after().is_some());
            assert_eq!(
                "this/is/a/test/path/file1001.txt",
                *contents.first().unwrap()
            );
        }
    }

    Ok(())
}

/// Listing a bucket that does not exist must answer NoSuchBucket -- and
/// must not create it. The list path used to open the bucket tree with
/// create-if-missing semantics, minting a bucket that accepted PUTs yet
/// never appeared in ListBuckets.
#[tokio::test]
#[tracing::instrument]
async fn test_list_of_absent_bucket_creates_nothing() -> Result<()> {
    let _guard = serial().await;

    let c = Client::new(setup_test(StorageEngine::Fjall, Some(1)));
    let ghost = format!("test-ghost-{}", Uuid::new_v4());

    let err = c.list_objects_v2().bucket(&ghost).send().await.unwrap_err();
    assert_eq!(error_code(&err), "NoSuchBucket");

    let err = c.list_objects().bucket(&ghost).send().await.unwrap_err();
    assert_eq!(error_code(&err), "NoSuchBucket");

    // The refused lists must not have materialized anything: a PUT into
    // the bucket still has nowhere to go.
    let err = c
        .put_object()
        .bucket(&ghost)
        .key("file.txt")
        .body(ByteStream::from_static(b"hello"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(error_code(&err), "NoSuchBucket");

    let buckets = c.list_buckets().send().await?;
    assert!(
        buckets
            .buckets()
            .iter()
            .filter_map(|b| b.name())
            .all(|name| name != ghost),
        "the ghost bucket must not appear in ListBuckets"
    );

    Ok(())
}

/// A delimiter listing rolls keys up into CommonPrefixes: each distinct
/// rolled-up prefix appears exactly once, counts against MaxKeys like an
/// object, and never reappears on a later page -- the page consumes the
/// whole group so the continuation token resumes past it.
#[tokio::test]
#[tracing::instrument]
async fn test_delimiter_listing_rolls_up_common_prefixes() -> Result<()> {
    let _guard = serial().await;

    let c = Client::new(setup_test(StorageEngine::Fjall, Some(1)));
    let bucket = format!("test-delim-{}", Uuid::new_v4());
    let bucket_str = bucket.as_str();
    create_bucket(&c, bucket_str).await?;

    for key in [
        "a/1.txt",
        "a/2.txt",
        "b/mid.txt",
        "b/x/deep.txt",
        "root1.txt",
        "root2.txt",
    ] {
        c.put_object()
            .bucket(bucket_str)
            .key(key)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await?;
    }

    // One page: two rolled-up prefixes, two loose objects.
    let all = c
        .list_objects_v2()
        .bucket(bucket_str)
        .delimiter("/")
        .send()
        .await?;
    let prefixes: Vec<_> = all
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    let keys: Vec<_> = all.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(prefixes, ["a/", "b/"]);
    assert_eq!(keys, ["root1.txt", "root2.txt"]);
    assert_eq!(all.key_count(), Some(4), "prefixes count like objects");
    assert_eq!(all.is_truncated(), Some(false));

    // Paged one item at a time: the sequence is a/, b/, root1, root2 with
    // no prefix repeated across pages.
    let mut token: Option<String> = None;
    let mut seq: Vec<String> = Vec::new();
    loop {
        let mut req = c
            .list_objects_v2()
            .bucket(bucket_str)
            .delimiter("/")
            .max_keys(1);
        if let Some(t) = &token {
            req = req.continuation_token(t);
        }
        let page = page_of(req.send().await?);
        seq.extend(page.0);
        token = page.1;
        if token.is_none() {
            break;
        }
        assert!(seq.len() <= 4, "pagination must terminate: {seq:?}");
    }
    assert_eq!(seq, ["a/", "b/", "root1.txt", "root2.txt"]);

    // Prefix and delimiter together: one loose key, one deeper roll-up.
    let under_b = c
        .list_objects_v2()
        .bucket(bucket_str)
        .prefix("b/")
        .delimiter("/")
        .send()
        .await?;
    let prefixes: Vec<_> = under_b
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    let keys: Vec<_> = under_b.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(prefixes, ["b/x/"]);
    assert_eq!(keys, ["b/mid.txt"]);

    // The v1 listing rolls up the same way and answers NextMarker when a
    // delimiter page truncates.
    let v1 = c
        .list_objects()
        .bucket(bucket_str)
        .delimiter("/")
        .max_keys(2)
        .send()
        .await?;
    let prefixes: Vec<_> = v1
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    assert_eq!(prefixes, ["a/", "b/"]);
    assert_eq!(v1.is_truncated(), Some(true));
    let marker = v1
        .next_marker()
        .expect("a truncated delimiter page names its marker");
    let v1_rest = c
        .list_objects()
        .bucket(bucket_str)
        .delimiter("/")
        .marker(marker)
        .send()
        .await?;
    let keys: Vec<_> = v1_rest.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, ["root1.txt", "root2.txt"]);
    assert!(v1_rest.common_prefixes().is_empty());

    Ok(())
}

/// The items of one ListObjectsV2 page in order (prefixes then keys is
/// fine here: a max-keys(1) page holds exactly one of them), plus the
/// continuation token when the page says it is truncated.
fn page_of(
    page: aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output,
) -> (Vec<String>, Option<String>) {
    let mut items: Vec<String> = page
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix().map(str::to_owned))
        .collect();
    items.extend(
        page.contents()
            .iter()
            .filter_map(|o| o.key().map(str::to_owned)),
    );
    assert!(
        items.len() <= 1,
        "a max-keys(1) page holds one item: {items:?}"
    );
    if page.is_truncated() == Some(true) {
        (items, page.next_continuation_token().map(str::to_owned))
    } else {
        (items, None)
    }
}
