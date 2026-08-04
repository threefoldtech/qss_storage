use s3s::host::SingleDomain;
use s3s::service::S3ServiceBuilder;

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
use std::sync::LazyLock;
use tokio::sync::Mutex;
use tokio::sync::MutexGuard;
use tracing::debug;
use uuid::Uuid;

pub(crate) const FS_ROOT: &str = concat!(env!("CARGO_TARGET_TMPDIR"), "/s3s-cas-test");
pub(crate) const DOMAIN_NAME: &str = "localhost:8014";
pub(crate) const REGION: &str = "us-west-2";

use s3cas::cas::StorageEngine;
pub(crate) const METADATA_DBS: [StorageEngine; 1] = [StorageEngine::Fjall];

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

pub(crate) fn setup_test(
    engine: s3cas::cas::StorageEngine,
    inlined_metadata_size: Option<usize>,
) -> &'static SdkConfig {
    *CONFIG_ENGINE.lock().unwrap() = Some(engine);
    *CONFIG_SIZE.lock().unwrap() = inlined_metadata_size;
    &CONFIG
}

pub(crate) async fn serial() -> MutexGuard<'static, ()> {
    static LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
    LOCK.lock().await
}

/// The SDK hands back the ETag exactly as it appears in the header, quotes
/// included for a strong ETag, so every assertion here strips them first.
pub(crate) fn unquote_e_tag(e_tag: &str) -> &str {
    e_tag.trim_matches('"')
}

pub(crate) fn is_lower_hex(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// A single-part ETag is the MD5 of the object content: 32 lowercase hex
/// digits, no suffix.
pub(crate) fn assert_single_part_e_tag(e_tag: &str) {
    let hash = unquote_e_tag(e_tag);
    assert_eq!(hash.len(), 32, "ETag is not a 32 hex digit MD5: {e_tag:?}");
    assert!(is_lower_hex(hash), "ETag is not lowercase hex: {e_tag:?}");
}

/// A multipart ETag is the MD5 of the concatenated part MD5s, suffixed with
/// the number of parts: `"{32 hex}-{N}"`.
pub(crate) fn assert_multipart_e_tag(e_tag: &str, expected_parts: usize) {
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

pub(crate) async fn create_bucket(c: &Client, bucket: &str) -> Result<()> {
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

pub(crate) async fn delete_object(c: &Client, bucket: &str, key: &str) -> Result<()> {
    c.delete_object().bucket(bucket).key(key).send().await?;
    Ok(())
}

pub(crate) async fn delete_bucket(c: &Client, bucket: &str) -> Result<()> {
    c.delete_bucket().bucket(bucket).send().await?;
    Ok(())
}

/// The error code S3 answered with, e.g. `NoSuchUpload`.
pub(crate) fn error_code<E: ProvideErrorMetadata, R>(err: &SdkError<E, R>) -> String {
    err.code().unwrap_or("<no code>").to_string()
}

/// Content unique to one test run, so its blocks are its own: a shared block
/// would keep a refcount (and its file) alive for reasons the test did not
/// arrange.
pub(crate) fn unique_content(tag: &str) -> String {
    format!("{tag} {}\n", Uuid::new_v4()).repeat(64)
}

/// The store's block files are named by the hex of their content hash, so a
/// payload nothing else in the suite writes can be looked for by name --
/// which is how these tests see whether a block survived an abort without
/// reaching into the store the running service holds open.
///
/// The walk skips `db`, the metadata database, which this test layout nests
/// inside the blocks root; nothing in it could match a 64 hex digit name
/// anyway. Temp residue is named `<hex>-<nonce>`, so it never matches either.
pub(crate) fn block_file_exists(data: &[u8]) -> bool {
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

pub(crate) async fn start_upload(c: &Client, bucket: &str, key: &str) -> Result<String> {
    let ans = c
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    Ok(ans.upload_id.expect("create returns an upload id"))
}

pub(crate) async fn upload_one_part(
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

pub(crate) async fn complete_upload(
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
pub(crate) async fn complete_upload_error(
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
