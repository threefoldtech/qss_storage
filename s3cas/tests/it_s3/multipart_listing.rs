use crate::common::{
    create_bucket, delete_bucket, serial, setup_test, start_upload, unique_content, upload_one_part,
};

use aws_sdk_s3::Client;

use anyhow::Result;
use s3cas::cas::StorageEngine;
use uuid::Uuid;

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
