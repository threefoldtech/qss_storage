use std::sync::Arc;

use crate::metastore::{BaseMetaTree, BlockID, MetaError, MetaTreeExt, Object, ObjectData};

pub trait TestStore {
    fn tree_open(&self, name: &str) -> Result<Arc<dyn BaseMetaTree>, MetaError>;
    fn get_bucket_ext(&self, name: &str) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError>;
}

pub fn test_get_bucket_keys(store: &impl TestStore) {
    let bucket_name = "testbucketkeys";

    // Setup bucket
    let bucket = store.tree_open(bucket_name).unwrap();

    // Insert test objects
    let test_keys = vec!["a", "b", "c"];
    for key in &test_keys {
        let obj = Object::new(
            1024,
            BlockID::from([1; 16]),
            ObjectData::SinglePart {
                blocks: vec![BlockID::from([1; 16])],
            },
        );
        bucket.insert(key.as_bytes(), obj.to_vec()).unwrap();
    }

    let bucket = store.get_bucket_ext(bucket_name).unwrap();

    let retrieved_keys: Vec<String> = bucket
        .iter_all()
        .map(|kv| String::from_utf8(kv.unwrap().0).unwrap())
        .collect();

    // Verify all keys present
    assert_eq!(retrieved_keys.len(), test_keys.len());
    for key in retrieved_keys {
        assert!(test_keys.contains(&key.as_str()));
    }

    // Test empty bucket
    let empty_bucket = "empty-bucket";
    let _ = store.tree_open(empty_bucket);
    let empty = store.get_bucket_ext(empty_bucket).unwrap();
    assert_eq!(empty.iter_all().count(), 0);
}

pub fn test_range_filter(store: &impl TestStore) {
    let bucket_name = "test-bucket";

    // Setup bucket
    let bucket = store.tree_open(bucket_name).unwrap();

    // Insert test objects with unordered keys
    let test_data = vec![
        ("c/1", "data5"),
        ("b/2", "data4"),
        ("a/1", "data1"),
        ("b/1", "data3"),
        ("a/2", "data2"),
    ];

    for (key, data) in &test_data {
        let obj = Object::new(
            data.len() as u64,
            BlockID::from([1; 16]),
            ObjectData::SinglePart {
                blocks: vec![BlockID::from([1; 16])],
            },
        );
        bucket.insert(key.as_bytes(), obj.to_vec()).unwrap();
    }

    let bucket = store.get_bucket_ext(bucket_name).unwrap();

    // Test cases
    {
        // 1. No filters
        let results: Vec<_> = bucket
            .range_filter(None, None, None)
            .map(|(k, _)| k)
            .collect();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0], "a/1");
    }

    {
        // 2. With start_after
        let results: Vec<_> = bucket
            .range_filter(Some("a/2".to_string()), None, None)
            .map(|(k, _)| k)
            .collect();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], "b/1");
    }

    {
        // 3. With prefix
        let results: Vec<_> = bucket
            .range_filter(None, Some("b".to_string()), None)
            .map(|(k, _)| k)
            .collect();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|k| k.starts_with("b/")));
    }

    {
        // 4. With continuation token
        let results: Vec<_> = bucket
            .range_filter(None, None, Some("b/1".to_string()))
            .map(|(k, _)| k)
            .collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], "b/2");
    }

    {
        // 5. With both start_after and continuation token
        let results: Vec<_> = bucket
            .range_filter(Some("b/1".to_string()), None, Some("a/2".to_string()))
            .map(|(k, _)| k)
            .collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], "b/2");
    }
    {
        // if start_after/continuation_token is greater than prefix, return empty

        // it is clearly greater than prefix
        let results: Vec<_> = bucket
            .range_filter(None, Some("b".to_string()), Some("c".to_string()))
            .map(|(k, _)| k)
            .collect();

        assert_eq!(results.len(), 0);

        // token < prefix, can be discarded
        let results: Vec<_> = bucket
            .range_filter(None, Some("b/".to_string()), Some("b".to_string()))
            .map(|(k, _)| k)
            .collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], "b/1");
        assert_eq!(results[1], "b/2");

        // token has prefix, token > prefix
        let results: Vec<_> = bucket
            .range_filter(None, Some("b/".to_string()), Some("b/0".to_string()))
            .map(|(k, _)| k)
            .collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], "b/1");
        assert_eq!(results[1], "b/2");

        // token has prefix, token > prefix
        let results: Vec<_> = bucket
            .range_filter(None, Some("b/".to_string()), Some("b/1".to_string()))
            .map(|(k, _)| k)
            .collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], "b/2");
    }
}
