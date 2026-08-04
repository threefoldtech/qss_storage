use std::sync::Arc;

use crate::metastore::{
    BaseMetaTree, BlockId, ContentHash, MetaError, MetaTreeExt, Object, ObjectData,
};

pub trait TestStore {
    fn tree_open(&self, name: &str) -> Result<Arc<dyn BaseMetaTree>, MetaError>;
    fn get_bucket_ext(&self, name: &str) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError>;
    fn list_trees(&self) -> Result<Vec<String>, MetaError>;
    // ---- tfstor-extension: BEGIN ----
    fn num_keys(&self, name: &str) -> Result<usize, MetaError>;
    // ---- tfstor-extension: END ----
}

/// Runs the shared backend battery against a concrete store type.
///
/// `$store` is the store type, `$setup` a closure returning
/// `($store, tempfile::TempDir)` -- the directory is returned so the caller
/// keeps it alive for the duration of the test. Both fjall backends invoke
/// this, which is what makes the battery a real cross-backend comparison
/// rather than two copies that can drift apart.
macro_rules! backend_test_battery {
    ($store:ty, $setup:expr) => {
        impl $crate::metastore::stores::test_utils::TestStore for $store {
            fn tree_open(
                &self,
                name: &str,
            ) -> Result<
                std::sync::Arc<dyn $crate::metastore::BaseMetaTree>,
                $crate::metastore::MetaError,
            > {
                <$store as $crate::metastore::Store>::tree_open(self, name)
            }

            fn get_bucket_ext(
                &self,
                name: &str,
            ) -> Result<
                std::sync::Arc<dyn $crate::metastore::MetaTreeExt + Send + Sync>,
                $crate::metastore::MetaError,
            > {
                <$store as $crate::metastore::Store>::tree_ext_open(self, name)
            }

            fn list_trees(&self) -> Result<Vec<String>, $crate::metastore::MetaError> {
                <$store as $crate::metastore::Store>::list_trees(self)
            }

            fn num_keys(&self, name: &str) -> Result<usize, $crate::metastore::MetaError> {
                <$store as $crate::metastore::Store>::num_keys(self, name)
            }
        }

        #[test]
        fn test_get_bucket_keys() {
            let (store, _dir) = ($setup)();
            $crate::metastore::stores::test_utils::test_get_bucket_keys(&store);
        }

        #[test]
        fn test_range_filter() {
            let (store, _dir) = ($setup)();
            $crate::metastore::stores::test_utils::test_range_filter(&store);
        }

        #[test]
        fn test_list_trees() {
            let (store, _dir) = ($setup)();
            $crate::metastore::stores::test_utils::test_list_trees(&store);
        }

        // ---- tfstor-extension: BEGIN ----
        #[test]
        fn test_num_keys() {
            let (store, _dir) = ($setup)();
            $crate::metastore::stores::test_utils::test_num_keys(&store);
        }

        #[test]
        fn test_iter_kv_is_a_mirror_of_iter_kv_backward() {
            let (store, _dir) = ($setup)();
            $crate::metastore::stores::test_utils::test_iter_kv_both_directions(&store);
        }
        // ---- tfstor-extension: END ----
    };
}

pub(crate) use backend_test_battery;

// ---- tfstor-extension: BEGIN ----
/// Guards against `Store::num_keys` regressing to `unimplemented!()` on any
/// backend: it must count the keys of a named tree, not panic.
pub fn test_num_keys(store: &impl TestStore) {
    let bucket_name = "test-num-keys";

    let bucket = store.tree_open(bucket_name).unwrap();
    assert_eq!(store.num_keys(bucket_name).unwrap(), 0);

    let test_keys = ["a", "b", "c", "d", "e"];
    for key in &test_keys {
        let obj = Object::new(
            1024,
            ContentHash::from([1; 16]),
            ObjectData::SinglePart {
                blocks: vec![BlockId::from([1; 16])],
            },
        );
        bucket.insert(key.as_bytes(), obj.to_vec()).unwrap();
    }

    assert_eq!(store.num_keys(bucket_name).unwrap(), test_keys.len());

    // A tree that was never written to still counts, and counts zero.
    assert_eq!(store.num_keys("test-num-keys-empty").unwrap(), 0);
}

/// The two directions respcas's SCAN and RSCAN are built on, and the
/// symmetry between them.
///
/// No cursor means "from the end I start at": the smallest key going forward,
/// the LARGEST key going backward. A cursor resumes strictly past itself, in
/// whichever direction is being walked. The backward half of that is what
/// `RSCAN 0` needs -- a no-cursor backward walk that ranged below the empty
/// key answered nothing at all, so a reverse enumeration could not be
/// started.
pub fn test_iter_kv_both_directions(store: &impl TestStore) {
    let bucket_name = "test-iter-kv";
    let tree = store.tree_open(bucket_name).unwrap();

    let keys = [
        b"a".to_vec(),
        b"b".to_vec(),
        b"c".to_vec(),
        b"d".to_vec(),
        b"e".to_vec(),
    ];
    for key in &keys {
        tree.insert(key, b"value".to_vec()).unwrap();
    }

    let tree = store.get_bucket_ext(bucket_name).unwrap();
    let walk = |iter: crate::metastore::KeyValuePairs| -> Vec<Vec<u8>> {
        iter.map(|kv| kv.unwrap().0).collect()
    };

    // No cursor: everything, from each end.
    assert_eq!(walk(tree.iter_kv(None)), keys.to_vec());
    let mut backward = keys.to_vec();
    backward.reverse();
    assert_eq!(
        walk(tree.iter_kv_backward(None)),
        backward,
        "a backward walk with no cursor starts at the largest key"
    );

    // A cursor is exclusive in both directions.
    assert_eq!(
        walk(tree.iter_kv(Some(b"c".to_vec()))),
        vec![b"d".to_vec(), b"e".to_vec()]
    );
    assert_eq!(
        walk(tree.iter_kv_backward(Some(b"c".to_vec()))),
        vec![b"b".to_vec(), b"a".to_vec()]
    );

    // The ends: past the last key in either direction is an empty walk, and
    // the empty key is a cursor like any other going forward.
    assert!(walk(tree.iter_kv(Some(b"e".to_vec()))).is_empty());
    assert!(walk(tree.iter_kv_backward(Some(b"a".to_vec()))).is_empty());
    assert_eq!(walk(tree.iter_kv(Some(Vec::new()))), keys.to_vec());

    // And a tree with nothing in it walks to nothing either way.
    let empty_name = "test-iter-kv-empty";
    let _ = store.tree_open(empty_name);
    let empty = store.get_bucket_ext(empty_name).unwrap();
    assert_eq!(empty.iter_kv(None).count(), 0);
    assert_eq!(empty.iter_kv_backward(None).count(), 0);
}
// ---- tfstor-extension: END ----

/// The enumeration ADR 0005's closed-holder-set rule rests on: every tree
/// that exists must be listed, whether or not anything else knows about it.
pub fn test_list_trees(store: &impl TestStore) {
    assert!(
        store.list_trees().unwrap().is_empty(),
        "a store with no trees lists none"
    );

    // A tree exists from the moment it is opened, reserved names included.
    for name in ["alpha", "beta", "_RESERVED"] {
        store.tree_open(name).unwrap();
    }

    let mut trees = store.list_trees().unwrap();
    trees.sort();
    assert_eq!(trees, vec!["_RESERVED", "alpha", "beta"]);
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
            ContentHash::from([1; 16]),
            ObjectData::SinglePart {
                blocks: vec![BlockId::from([1; 16])],
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
            ContentHash::from([1; 16]),
            ObjectData::SinglePart {
                blocks: vec![BlockId::from([1; 16])],
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
