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
use aws_sdk_s3::primitives::ByteStream;

use aws_sdk_s3::types::BucketLocationConstraint;
//use aws_sdk_s3::types::ChecksumMode;
use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_sdk_s3::types::CompletedPart;
use aws_sdk_s3::types::CreateBucketConfiguration;

use anyhow::Result;
use once_cell::sync::Lazy;
use std::convert::TryInto;
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

static CONFIG: Lazy<SdkConfig> = Lazy::new(|| {
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
    let casfs = s3cas::cas::CasFS::single_namespace(
        FS_ROOT.into(),
        FS_ROOT.into(),
        metrics.to_cas(),
        storage_engine,
        inlined_size,
        None,
    )
    .expect("can construct CasFS");
    let s3fs = s3cas::s3fs::S3FS::new(casfs, metrics.clone());

    // Setup S3 service
    let service = {
        let mut b = S3ServiceBuilder::new(s3fs);
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
    static LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
    LOCK.lock().await
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
        (s3cas::cas::StorageEngine::FjallNotx, Some(1)),
        (s3cas::cas::StorageEngine::FjallNotx, Some(10240000)),
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
        c.put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            //.checksum_crc32_c(crc32c.as_str())
            .send()
            .await?;

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
const METADATA_DBS: [StorageEngine; 2] = [StorageEngine::Fjall, StorageEngine::FjallNotx];
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
            assert!(response.start_after().is_some());
            assert_eq!(
                "this/is/a/test/path/file1001.txt",
                *contents.first().unwrap()
            );
        }
    }

    Ok(())
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

        let part = CompletedPart::builder()
            .e_tag(ans.e_tag.unwrap_or_default())
            .part_number(part_number)
            .build();

        vec![part]
    };

    {
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(upload_parts))
            .build();

        let _ = c
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .multipart_upload(upload)
            .upload_id(upload_id)
            .send()
            .await?;
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

async fn delete_object(c: &Client, bucket: &str, key: &str) -> Result<()> {
    c.delete_object().bucket(bucket).key(key).send().await?;
    Ok(())
}

async fn delete_bucket(c: &Client, bucket: &str) -> Result<()> {
    c.delete_bucket().bucket(bucket).send().await?;
    Ok(())
}
