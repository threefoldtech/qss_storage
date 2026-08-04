use crate::common::{
    assert_single_part_e_tag, create_bucket, delete_bucket, serial, setup_test, unquote_e_tag,
};

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;

use anyhow::Result;
use std::convert::TryInto;
use uuid::Uuid;

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
