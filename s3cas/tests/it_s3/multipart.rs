use crate::common::{
    METADATA_DBS, assert_multipart_e_tag, assert_single_part_e_tag, block_file_exists,
    complete_upload, complete_upload_error, create_bucket, delete_bucket, delete_object,
    error_code, serial, setup_test, start_upload, unique_content, unquote_e_tag, upload_one_part,
};

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_sdk_s3::types::CompletedPart;

use anyhow::Result;
use s3cas::cas::StorageEngine;
use std::convert::TryInto;
use uuid::Uuid;

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
