#![forbid(unsafe_code)]
#![deny(
    clippy::all, //
    clippy::must_use_candidate, //
)]

use s3s::host::SingleDomain;
use s3s::service::S3ServiceBuilder;

use std::env;

use aws_config::SdkConfig;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;

use aws_sdk_s3::types::BucketLocationConstraint;
//use aws_sdk_s3::types::ChecksumMode;
use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_sdk_s3::types::CompletedPart;
use aws_sdk_s3::types::CreateBucketConfiguration;

use anyhow::Result;
use std::convert::TryInto;
use std::sync::LazyLock;
use tokio::sync::Mutex;
use tokio::sync::MutexGuard;
use tracing::{debug, error};
use uuid::Uuid;

const FS_ROOT: &str = concat!(env!("CARGO_TARGET_TMPDIR"), "/s3s-cas-test");
const DOMAIN_NAME: &str = "localhost:8014";
const REGION: &str = "us-west-2";

pub fn setup_tracing() {
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

macro_rules! log_and_unwrap {
    ($result:expr) => {
        match $result {
            Ok(ans) => {
                debug!(?ans);
                ans
            }
            Err(err) => {
                error!(?err);
                return Err(err.into());
            }
        }
    };
}

use std::sync::Mutex as StdMutex;

// Create a static CONFIG_SIZE to store the inlined size
static CONFIG_SIZE: StdMutex<Option<usize>> = StdMutex::new(None);
static CONFIG_ENGINE: StdMutex<Option<s3cas::cas::StorageEngine>> = StdMutex::new(None);

static CONFIG: LazyLock<SdkConfig> = LazyLock::new(|| {
    setup_tracing();

    // Fake credentials
    let cred = Credentials::for_tests();

    let metrics = s3cas::metrics::SharedMetrics::new();
    let storage_engine = CONFIG_ENGINE
        .lock()
        .unwrap()
        .as_ref()
        .cloned()
        .unwrap_or(s3cas::cas::StorageEngine::Fjall);
    let inlined_size = CONFIG_SIZE.lock().unwrap().or(Some(1));

    // Start from an empty directory. The store carries a format header now,
    // so a store left behind by an older build is refused at open instead of
    // being reused -- which is the point of the header, but it would leave
    // this suite failing on a stale target/ directory rather than on
    // anything it tests.
    let _ = std::fs::remove_dir_all(FS_ROOT);

    let casfs = s3cas::cas::CasFS::single_namespace(
        FS_ROOT.into(),
        FS_ROOT.into(),
        metrics.to_cas(),
        // Everything else is the built-in default: stripe count, batch cap,
        // no commit station, no read verification.
        cas_storage::StoreOptions {
            metadata_db: storage_engine,
            inline_metadata_size: inlined_size,
            ..cas_storage::StoreOptions::default()
        },
    )
    .expect("can construct CasFS");
    let s3 = s3cas::api::S3Cas::new(casfs, metrics.clone());

    // Setup S3 service
    let service = {
        let mut b = S3ServiceBuilder::new(s3);
        b.set_auth(s3s::auth::SimpleAuth::from_single(
            cred.access_key_id(),
            cred.secret_access_key(),
        ));
        b.set_host(SingleDomain::new(DOMAIN_NAME).unwrap());
        b.build()
    };

    // Convert to aws http client
    let client = s3s_aws::Client::from(service);

    // Setup aws sdk config
    SdkConfig::builder()
        .credentials_provider(SharedCredentialsProvider::new(cred))
        .http_client(client)
        .region(Region::new(REGION))
        .endpoint_url(format!("http://{DOMAIN_NAME}"))
        .build()
});

fn setup_test(
    engine: s3cas::cas::StorageEngine,
    inlined_metadata_size: Option<usize>,
) -> &'static SdkConfig {
    *CONFIG_ENGINE.lock().unwrap() = Some(engine);
    *CONFIG_SIZE.lock().unwrap() = inlined_metadata_size;
    &CONFIG
}

async fn serial() -> MutexGuard<'static, ()> {
    static LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
    LOCK.lock().await
}

/// The SDK hands back the ETag exactly as it appears in the header, quotes
/// included for a strong ETag, so every assertion here strips them first.
fn unquote_e_tag(e_tag: &str) -> &str {
    e_tag.trim_matches('"')
}

fn is_lower_hex(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// A single-part ETag is the MD5 of the object content: 32 lowercase hex
/// digits, no suffix.
fn assert_single_part_e_tag(e_tag: &str) {
    let hash = unquote_e_tag(e_tag);
    assert_eq!(hash.len(), 32, "ETag is not a 32 hex digit MD5: {e_tag:?}");
    assert!(is_lower_hex(hash), "ETag is not lowercase hex: {e_tag:?}");
}

/// A multipart ETag is the MD5 of the concatenated part MD5s, suffixed with
/// the number of parts: `"{32 hex}-{N}"`.
fn assert_multipart_e_tag(e_tag: &str, expected_parts: usize) {
    let value = unquote_e_tag(e_tag);
    let (hash, parts) = value
        .split_once('-')
        .unwrap_or_else(|| panic!("multipart ETag has no part count suffix: {e_tag:?}"));
    assert_eq!(hash.len(), 32, "ETag is not a 32 hex digit MD5: {e_tag:?}");
    assert!(is_lower_hex(hash), "ETag is not lowercase hex: {e_tag:?}");
    let parts: usize = parts
        .parse()
        .unwrap_or_else(|_| panic!("multipart ETag part count is not a number: {e_tag:?}"));
    assert_eq!(parts, expected_parts, "wrong part count in ETag {e_tag:?}");
}

async fn create_bucket(c: &Client, bucket: &str) -> Result<()> {
    let location = BucketLocationConstraint::from(REGION);
    let cfg = CreateBucketConfiguration::builder()
        .location_constraint(location)
        .build();

    c.create_bucket()
        .create_bucket_configuration(cfg)
        .bucket(bucket)
        .send()
        .await?;

    debug!("created bucket: {bucket:?}");
    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_put_delete_object() -> Result<()> {
    let test_cases = [
        (s3cas::cas::StorageEngine::Fjall, Some(1)),
        (s3cas::cas::StorageEngine::Fjall, Some(10240000)),
    ];

    for (engine, size) in test_cases {
        do_test_put_delete_object(engine, size).await?;
    }
    Ok(())
}

async fn do_test_put_delete_object(
    engine: s3cas::cas::StorageEngine,
    inlined_metadata_size: Option<usize>,
) -> Result<()> {
    let _guard = serial().await;

    let c = Client::new(setup_test(engine, inlined_metadata_size));
    let bucket = format!("test-single-object-{}", Uuid::new_v4());
    let bucket = bucket.as_str();
    let key = "sample.txt";
    let content = "hello hello hello hello hello hello hello\n";
    //let crc32c =
    //    base64_simd::STANDARD.encode_to_string(crc32c::crc32c(content.as_bytes()).to_be_bytes());

    create_bucket(&c, bucket).await?;

    // happy path
    {
        // put the object
        let body = ByteStream::from_static(content.as_bytes());
        let put = c
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            //.checksum_crc32_c(crc32c.as_str())
            .send()
            .await?;

        // a single part object gets the plain MD5 of its content as ETag
        assert_single_part_e_tag(put.e_tag().expect("put returns an ETag"));

        // get the object
        let ans = c
            .get_object()
            .bucket(bucket)
            .key(key)
            //.checksum_mode(ChecksumMode::Enabled)
            .send()
            .await?;

        // checkings
        let content_length: usize = ans.content_length().unwrap().try_into().unwrap();
        //let checksum_crc32c = ans.checksum_crc32_c.unwrap();
        let body = ans.body.collect().await?.into_bytes();

        assert_eq!(content_length, content.len());
        //assert_eq!(checksum_crc32c, crc32c);
        assert_eq!(body.as_ref(), content.as_bytes());
    }

    {
        // an empty object still gets the MD5 of zero bytes as ETag
        let empty_key = "empty.txt";
        let put = c
            .put_object()
            .bucket(bucket)
            .key(empty_key)
            .body(ByteStream::from_static(b""))
            .send()
            .await?;
        assert_eq!(
            unquote_e_tag(put.e_tag().expect("put returns an ETag")),
            "d41d8cd98f00b204e9800998ecf8427e"
        );

        c.delete_object()
            .bucket(bucket)
            .key(empty_key)
            .send()
            .await?;
    }

    {
        // put to non existent bucket

        // put the object
        let body = ByteStream::from_static(content.as_bytes());
        let result = c
            .put_object()
            .bucket("non-existent-buckett")
            .key(key)
            .body(body)
            //.checksum_crc32_c(crc32c.as_str())
            .send()
            .await;
        assert!(result.is_err());
    }

    {
        // delete the object
        let result = c
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(result.delete_marker().is_none());

        // delete non existent object

        let result = c.delete_object().bucket(bucket).key(key).send().await;
        assert!(result.is_err());
    }

    // cleanup
    delete_bucket(&c, bucket).await?;

    Ok(())
}

use s3cas::cas::StorageEngine;
const METADATA_DBS: [StorageEngine; 1] = [StorageEngine::Fjall];
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

#[tokio::test]
#[tracing::instrument]
async fn test_multipart() -> Result<()> {
    for engine in METADATA_DBS {
        do_test_multipart(engine).await?;
    }
    Ok(())
}

async fn do_test_multipart(engine: StorageEngine) -> Result<()> {
    let _guard = serial().await;

    let c = Client::new(setup_test(engine, Some(1)));

    let bucket = format!("test-multipart-{}", Uuid::new_v4());
    let bucket = bucket.as_str();
    create_bucket(&c, bucket).await?;

    let key = "sample.txt";
    let content = "abcdefghijklmnopqrstuvwxyz/0123456789/!@#$%^&*();\n";

    let upload_id = {
        let ans = c
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        ans.upload_id.unwrap()
    };
    assert_ne!(upload_id.len(), 0);
    let upload_id = upload_id.as_str();

    let upload_parts = {
        let body = ByteStream::from_static(content.as_bytes());
        let part_number = 1;

        let ans = c
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .body(body)
            .part_number(part_number)
            .send()
            .await?;

        // an uploaded part is identified by the plain MD5 of its content
        let part_e_tag = ans.e_tag.unwrap_or_default();
        assert_single_part_e_tag(&part_e_tag);

        let part = CompletedPart::builder()
            .e_tag(part_e_tag)
            .part_number(part_number)
            .build();

        vec![part]
    };

    {
        let part_count = upload_parts.len();
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(upload_parts))
            .build();

        let ans = c
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .multipart_upload(upload)
            .upload_id(upload_id)
            .send()
            .await?;

        // the completed object carries the multipart ETag: MD5 of the
        // concatenated part MD5s, suffixed with the part count
        assert_multipart_e_tag(
            ans.e_tag().expect("complete multipart returns an ETag"),
            part_count,
        );
    }

    {
        let ans = c.get_object().bucket(bucket).key(key).send().await?;

        let content_length: usize = ans.content_length().unwrap().try_into().unwrap();
        let body = ans.body.collect().await?.into_bytes();

        assert_eq!(content_length, content.len());
        assert_eq!(body.as_ref(), content.as_bytes());
    }

    {
        delete_object(&c, bucket, key).await?;
        delete_bucket(&c, bucket).await?;
    }

    Ok(())
}

/// The store's block files are named by the hex of their content hash, so a
/// payload nothing else in the suite writes can be looked for by name --
/// which is how these tests see whether a block survived an abort without
/// reaching into the store the running service holds open.
///
/// The walk skips `db`, the metadata database, which this test layout nests
/// inside the blocks root; nothing in it could match a 64 hex digit name
/// anyway. Temp residue is named `<hex>-<nonce>`, so it never matches either.
fn block_file_exists(data: &[u8]) -> bool {
    fn walk(dir: &std::path::Path, name: &str) -> bool {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return false;
        };
        for entry in entries.flatten() {
            if entry.file_name() == "db" {
                continue;
            }
            if entry.path().is_dir() {
                if walk(&entry.path(), name) {
                    return true;
                }
            } else if entry.file_name() == *name {
                return true;
            }
        }
        false
    }

    let hex = s3cas::cas::Hasher::Blake3W32.hash(data).to_hex();
    walk(&std::path::Path::new(FS_ROOT).join("blocks"), &hex)
}

/// The error code S3 answered with, e.g. `NoSuchUpload`.
fn error_code<E: ProvideErrorMetadata, R>(err: &SdkError<E, R>) -> String {
    err.code().unwrap_or("<no code>").to_string()
}

/// Content unique to one test run, so its blocks are its own: a shared block
/// would keep a refcount (and its file) alive for reasons the test did not
/// arrange.
fn unique_content(tag: &str) -> String {
    format!("{tag} {}\n", Uuid::new_v4()).repeat(64)
}

async fn start_upload(c: &Client, bucket: &str, key: &str) -> Result<String> {
    let ans = c
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    Ok(ans.upload_id.expect("create returns an upload id"))
}

async fn upload_one_part(
    c: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    content: &str,
) -> Result<CompletedPart> {
    let ans = c
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .body(ByteStream::from(content.as_bytes().to_vec()))
        .send()
        .await?;
    Ok(CompletedPart::builder()
        .e_tag(ans.e_tag.unwrap_or_default())
        .part_number(part_number)
        .build())
}

async fn complete_upload(
    c: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: Vec<CompletedPart>,
) -> Result<aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput> {
    let upload = CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    let ans = c
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(upload)
        .send()
        .await?;
    Ok(ans)
}

/// The S3 error code a complete that must fail failed with.
async fn complete_upload_error(
    c: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: Vec<CompletedPart>,
) -> String {
    let upload = CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    let err = c
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(upload)
        .send()
        .await
        .expect_err("this complete must fail");
    error_code(&err)
}

/// Abort reaps the upload: its parts stop being listable, and the blocks
/// only they referenced lose their files (ADR 0003). Everything that comes
/// after an abort -- a second abort, a listing, another part, a complete --
/// is told `NoSuchUpload`, because the claim took the record away.
#[tokio::test]
#[tracing::instrument]
async fn test_multipart_abort() -> Result<()> {
    let _guard = serial().await;

    let c = Client::new(setup_test(StorageEngine::Fjall, Some(1)));
    let bucket = format!("test-abort-{}", Uuid::new_v4());
    let bucket = bucket.as_str();
    create_bucket(&c, bucket).await?;

    let key = "aborted.txt";
    let content = unique_content("abort me");
    let upload_id = start_upload(&c, bucket, key).await?;
    let part = upload_one_part(&c, bucket, key, &upload_id, 1, &content).await?;
    assert!(
        block_file_exists(content.as_bytes()),
        "the uploaded part's block must be on disk"
    );

    // The part is listable while the upload lives.
    let listed = c
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .send()
        .await?;
    assert_eq!(listed.parts().len(), 1);
    assert_eq!(listed.parts()[0].part_number(), Some(1));
    assert_eq!(
        listed.parts()[0].size(),
        Some(content.len().try_into().unwrap())
    );
    assert_eq!(
        unquote_e_tag(listed.parts()[0].e_tag().unwrap()),
        unquote_e_tag(part.e_tag().unwrap()),
        "ListParts must echo the ETag UploadPart answered"
    );

    c.abort_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .send()
        .await?;

    assert!(
        !block_file_exists(content.as_bytes()),
        "abort must drop the part's last reference and unlink its file"
    );

    // Everything after the claim is NoSuchUpload.
    let err = c
        .abort_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .send()
        .await
        .unwrap_err();
    assert_eq!(error_code(&err), "NoSuchUpload", "the second abort lost");

    let err = c
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .send()
        .await
        .unwrap_err();
    assert_eq!(error_code(&err), "NoSuchUpload");

    assert_eq!(
        complete_upload_error(&c, bucket, key, &upload_id, vec![part]).await,
        "NoSuchUpload",
        "complete after abort finds no record"
    );

    delete_bucket(&c, bucket).await?;
    Ok(())
}

/// Complete and abort race for one record and exactly one of them wins,
/// whichever order they arrive in: the loser is told `NoSuchUpload` both
/// ways.
#[tokio::test]
#[tracing::instrument]
async fn test_multipart_complete_and_abort_are_exclusive() -> Result<()> {
    let _guard = serial().await;

    let c = Client::new(setup_test(StorageEngine::Fjall, Some(1)));
    let bucket = format!("test-claim-{}", Uuid::new_v4());
    let bucket = bucket.as_str();
    create_bucket(&c, bucket).await?;

    // Complete first: the abort that follows finds nothing to claim.
    let key = "completed.txt";
    let content = unique_content("complete then abort");
    let upload_id = start_upload(&c, bucket, key).await?;
    let part = upload_one_part(&c, bucket, key, &upload_id, 1, &content).await?;
    complete_upload(&c, bucket, key, &upload_id, vec![part]).await?;

    let err = c
        .abort_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .send()
        .await
        .unwrap_err();
    assert_eq!(error_code(&err), "NoSuchUpload");
    // The completed object is untouched by the losing abort.
    let ans = c.get_object().bucket(bucket).key(key).send().await?;
    assert_eq!(
        ans.body.collect().await?.into_bytes().as_ref(),
        content.as_bytes()
    );

    // Abort first: the complete that follows loses the same way.
    let key = "raced.txt";
    let content = unique_content("abort then complete");
    let upload_id = start_upload(&c, bucket, key).await?;
    let part = upload_one_part(&c, bucket, key, &upload_id, 1, &content).await?;
    c.abort_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .send()
        .await?;

    assert_eq!(
        complete_upload_error(&c, bucket, key, &upload_id, vec![part]).await,
        "NoSuchUpload"
    );
    assert!(
        c.get_object().bucket(bucket).key(key).send().await.is_err(),
        "the losing complete must not have minted an object"
    );

    delete_object(&c, bucket, "completed.txt").await?;
    delete_bucket(&c, bucket).await?;
    Ok(())
}

/// An unknown upload id is refused before a single block is written -- the
/// check that makes abort semantics coherent (ADR 0003 decision 3).
#[tokio::test]
#[tracing::instrument]
async fn test_upload_part_to_unknown_upload() -> Result<()> {
    let _guard = serial().await;

    let c = Client::new(setup_test(StorageEngine::Fjall, Some(1)));
    let bucket = format!("test-unknown-upload-{}", Uuid::new_v4());
    let bucket = bucket.as_str();
    create_bucket(&c, bucket).await?;

    let content = unique_content("never stored");
    let err = c
        .upload_part()
        .bucket(bucket)
        .key("ghost.txt")
        .upload_id(Uuid::new_v4().to_string())
        .part_number(1)
        .body(ByteStream::from(content.as_bytes().to_vec()))
        .send()
        .await
        .unwrap_err();

    assert_eq!(error_code(&err), "NoSuchUpload");
    assert!(
        !block_file_exists(content.as_bytes()),
        "the refused part must not have written any block"
    );

    delete_bucket(&c, bucket).await?;
    Ok(())
}

/// A complete with no parts used to mint an empty object; it is now a
/// malformed request.
#[tokio::test]
#[tracing::instrument]
async fn test_complete_with_no_parts_is_rejected() -> Result<()> {
    let _guard = serial().await;

    let c = Client::new(setup_test(StorageEngine::Fjall, Some(1)));
    let bucket = format!("test-empty-complete-{}", Uuid::new_v4());
    let bucket = bucket.as_str();
    create_bucket(&c, bucket).await?;

    let key = "nothing.txt";
    let upload_id = start_upload(&c, bucket, key).await?;

    assert_eq!(
        complete_upload_error(&c, bucket, key, &upload_id, vec![]).await,
        "InvalidRequest"
    );

    assert!(
        c.get_object().bucket(bucket).key(key).send().await.is_err(),
        "no object may have been minted"
    );
    // The rejected complete claimed nothing: the upload is still there.
    c.abort_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .send()
        .await?;

    delete_bucket(&c, bucket).await?;
    Ok(())
}

/// A complete naming a part that was never uploaded is refused WITHOUT
/// consuming the upload: validation runs before the claim, so the client can
/// send a corrected complete and have it succeed.
#[tokio::test]
#[tracing::instrument]
async fn test_complete_with_a_missing_part_leaves_the_upload_intact() -> Result<()> {
    let _guard = serial().await;

    let c = Client::new(setup_test(StorageEngine::Fjall, Some(1)));
    let bucket = format!("test-missing-part-{}", Uuid::new_v4());
    let bucket = bucket.as_str();
    create_bucket(&c, bucket).await?;

    let key = "retried.txt";
    let content = unique_content("only part one");
    let upload_id = start_upload(&c, bucket, key).await?;
    let part = upload_one_part(&c, bucket, key, &upload_id, 1, &content).await?;

    // Part 2 was never uploaded.
    let ghost = CompletedPart::builder()
        .e_tag("\"d41d8cd98f00b204e9800998ecf8427e\"")
        .part_number(2)
        .build();
    assert_eq!(
        complete_upload_error(
            &c,
            bucket,
            key,
            &upload_id,
            vec![part.clone(), ghost.clone()],
        )
        .await,
        "InvalidArgument"
    );

    // The upload survived the rejection, parts and all.
    let listed = c
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .send()
        .await?;
    assert_eq!(listed.parts().len(), 1, "the uploaded part is still there");

    // ...and the corrected complete succeeds.
    let ans = complete_upload(&c, bucket, key, &upload_id, vec![part]).await?;
    assert_multipart_e_tag(ans.e_tag().expect("complete returns an ETag"), 1);
    let got = c.get_object().bucket(bucket).key(key).send().await?;
    assert_eq!(
        got.body.collect().await?.into_bytes().as_ref(),
        content.as_bytes()
    );

    delete_object(&c, bucket, key).await?;
    delete_bucket(&c, bucket).await?;
    Ok(())
}

/// `ListParts` pages in part_number order, honoring the marker and the
/// maximum, and says so in `is_truncated` / `next_part_number_marker`.
#[tokio::test]
#[tracing::instrument]
async fn test_list_parts_order_and_pagination() -> Result<()> {
    let _guard = serial().await;

    let c = Client::new(setup_test(StorageEngine::Fjall, Some(1)));
    let bucket = format!("test-list-parts-{}", Uuid::new_v4());
    let bucket = bucket.as_str();
    create_bucket(&c, bucket).await?;

    let key = "paged.txt";
    let upload_id = start_upload(&c, bucket, key).await?;
    // Uploaded out of order: the listing order comes from the sort, not from
    // the order the parts arrived in.
    for part_number in [3, 1, 5, 2, 4] {
        let content = unique_content(&format!("part {part_number}"));
        upload_one_part(&c, bucket, key, &upload_id, part_number, &content).await?;
    }

    let numbers = |ans: &aws_sdk_s3::operation::list_parts::ListPartsOutput| -> Vec<i32> {
        ans.parts().iter().filter_map(|p| p.part_number()).collect()
    };

    let all = c
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .send()
        .await?;
    assert_eq!(numbers(&all), [1, 2, 3, 4, 5]);
    assert_eq!(all.is_truncated(), Some(false));
    assert!(all.next_part_number_marker().is_none());
    assert_eq!(all.max_parts(), Some(1000));

    let first = c
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .max_parts(2)
        .send()
        .await?;
    assert_eq!(numbers(&first), [1, 2]);
    assert_eq!(first.is_truncated(), Some(true));
    assert_eq!(first.next_part_number_marker(), Some("2"));

    let second = c
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .max_parts(2)
        .part_number_marker(first.next_part_number_marker().unwrap())
        .send()
        .await?;
    assert_eq!(numbers(&second), [3, 4]);
    assert_eq!(second.is_truncated(), Some(true));
    assert_eq!(second.part_number_marker(), Some("2"));

    let last = c
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .max_parts(2)
        .part_number_marker(second.next_part_number_marker().unwrap())
        .send()
        .await?;
    assert_eq!(numbers(&last), [5]);
    assert_eq!(last.is_truncated(), Some(false));
    assert!(last.next_part_number_marker().is_none());

    c.abort_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .send()
        .await?;
    delete_bucket(&c, bucket).await?;
    Ok(())
}

/// `ListMultipartUploads` reports one bucket's in-flight uploads in S3 order
/// -- key, then upload id -- filtered by prefix and paged by the two markers.
#[tokio::test]
#[tracing::instrument]
async fn test_list_multipart_uploads() -> Result<()> {
    let _guard = serial().await;

    let c = Client::new(setup_test(StorageEngine::Fjall, Some(1)));
    let bucket = format!("test-list-uploads-{}", Uuid::new_v4());
    let bucket = bucket.as_str();
    create_bucket(&c, bucket).await?;

    // Two uploads of one key (S3 allows any number) plus two other keys,
    // started in an order that is not the listing order.
    let mut started: Vec<(String, String)> = Vec::new();
    for key in ["b/second.txt", "a/first.txt", "a/first.txt", "c/third.txt"] {
        let upload_id = start_upload(&c, bucket, key).await?;
        started.push((key.to_string(), upload_id));
    }
    let mut expected: Vec<(String, String)> = started.clone();
    expected.sort();

    let pairs = |ans: &aws_sdk_s3::operation::list_multipart_uploads::ListMultipartUploadsOutput| -> Vec<(String, String)> {
        ans.uploads()
            .iter()
            .map(|u| {
                (
                    u.key().unwrap().to_string(),
                    u.upload_id().unwrap().to_string(),
                )
            })
            .collect()
    };

    let all = c.list_multipart_uploads().bucket(bucket).send().await?;
    assert_eq!(pairs(&all), expected, "key order, then upload id order");
    assert_eq!(all.is_truncated(), Some(false));
    assert!(
        all.uploads().iter().all(|u| u.initiated().is_some()),
        "every upload reports when it was initiated"
    );

    let prefixed = c
        .list_multipart_uploads()
        .bucket(bucket)
        .prefix("a/")
        .send()
        .await?;
    assert_eq!(
        pairs(&prefixed),
        expected
            .iter()
            .filter(|(key, _)| key.starts_with("a/"))
            .cloned()
            .collect::<Vec<_>>()
    );
    assert_eq!(prefixed.prefix(), Some("a/"));

    // Page one upload at a time through the two markers.
    let mut seen: Vec<(String, String)> = Vec::new();
    let mut key_marker: Option<String> = None;
    let mut upload_id_marker: Option<String> = None;
    loop {
        let page = c
            .list_multipart_uploads()
            .bucket(bucket)
            .max_uploads(1)
            .set_key_marker(key_marker.clone())
            .set_upload_id_marker(upload_id_marker.clone())
            .send()
            .await?;
        seen.extend(pairs(&page));
        if page.is_truncated() != Some(true) {
            break;
        }
        key_marker = page.next_key_marker().map(str::to_string);
        upload_id_marker = page.next_upload_id_marker().map(str::to_string);
        assert!(key_marker.is_some() && upload_id_marker.is_some());
    }
    assert_eq!(seen, expected, "paging one at a time sees each upload once");

    // A bucket of its own: the listing is per bucket, not store-wide.
    let other = format!("test-list-uploads-empty-{}", Uuid::new_v4());
    create_bucket(&c, &other).await?;
    let empty = c.list_multipart_uploads().bucket(&other).send().await?;
    assert!(empty.uploads().is_empty());
    delete_bucket(&c, &other).await?;

    for (key, upload_id) in started {
        c.abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await?;
    }
    assert!(
        c.list_multipart_uploads()
            .bucket(bucket)
            .send()
            .await?
            .uploads()
            .is_empty(),
        "aborting every upload empties the listing"
    );

    delete_bucket(&c, bucket).await?;
    Ok(())
}

async fn delete_object(c: &Client, bucket: &str, key: &str) -> Result<()> {
    c.delete_object().bucket(bucket).key(key).send().await?;
    Ok(())
}

async fn delete_bucket(c: &Client, bucket: &str) -> Result<()> {
    c.delete_bucket().bucket(bucket).send().await?;
    Ok(())
}
