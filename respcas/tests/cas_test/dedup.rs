use crate::common::{TestServer, block_count, block_files, open_store, rc_of, record};
use crate::helpers::*;

/// A dedup hit in the SAME namespace is a pure acknowledgement: the record
/// does not move, and the bytes the client sent are discarded unread. The
/// visible consequence is the ADR's deliberate casualty -- wrong bytes under
/// a present address are answered +OK, and GET still returns the verified
/// content.
#[test]
fn a_dedup_hit_in_this_namespace_acks_and_discards_the_bytes() {
    let server = TestServer::new();
    let mut conn = server.connect();
    select_cas(&mut conn, "blobs");

    let value = b"stored once, sent twice".to_vec();
    let key = address(&value);
    assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");

    let before: i64 = redis::cmd("KEYTIME")
        .arg(&key)
        .query(&mut conn)
        .expect("KEYTIME must answer");

    // The same content again: idempotent success.
    assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");

    // Wrong bytes under an address that is already present: also +OK,
    // because nothing hashes data it is about to discard.
    assert_eq!(set(&mut conn, &key, b"not the content").unwrap(), "OK");

    assert_eq!(
        get(&mut conn, &key).as_deref(),
        Some(&value[..]),
        "the store cannot be poisoned: the record was never rewritten"
    );
    let after: i64 = redis::cmd("KEYTIME")
        .arg(&key)
        .query(&mut conn)
        .expect("KEYTIME must answer");
    assert_eq!(before, after, "nothing was written");
    let checked: i64 = redis::cmd("CHECK").arg(&key).query(&mut conn).unwrap();
    assert_eq!(checked, 1);
}

/// A hit in ANOTHER content-addressed namespace is served by reference: the
/// second namespace gets a record of its own naming the same blocks, and a
/// DELETE in one leaves the other readable.
#[test]
fn a_hit_in_another_cas_namespace_is_cloned_by_reference() {
    let server = TestServer::new();
    let mut conn = server.connect();

    cas_namespace(&mut conn, "first");
    cas_namespace(&mut conn, "second");

    let value = big_value(0x33);
    let key = address(&value);

    let _: String = redis::cmd("SELECT").arg("first").query(&mut conn).unwrap();
    assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");

    // The second namespace does not have it yet: EXISTS is namespace-scoped.
    let _: String = redis::cmd("SELECT").arg("second").query(&mut conn).unwrap();
    assert!(!exists(&mut conn, &key));

    // Storing it here takes a reference to the content that is already in
    // the store; the bytes on the wire are discarded.
    assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");
    assert_eq!(get(&mut conn, &key).as_deref(), Some(&value[..]));

    // Now delete it in the first namespace. The second still reads.
    let _: String = redis::cmd("SELECT").arg("first").query(&mut conn).unwrap();
    let removed: i64 = redis::cmd("DEL").arg(&key).query(&mut conn).unwrap();
    assert_eq!(removed, 1);
    assert!(!exists(&mut conn, &key));

    let _: String = redis::cmd("SELECT").arg("second").query(&mut conn).unwrap();
    assert_eq!(
        get(&mut conn, &key).as_deref(),
        Some(&value[..]),
        "each namespace holds its own reference: one DEL cannot strand the other"
    );
    let checked: i64 = redis::cmd("CHECK").arg(&key).query(&mut conn).unwrap();
    assert_eq!(checked, 1, "the clone's blocks are intact");

    // The last holder frees it.
    let removed: i64 = redis::cmd("DEL").arg(&key).query(&mut conn).unwrap();
    assert_eq!(removed, 1);
    assert!(get(&mut conn, &key).is_none());
}

/// A 32-byte key in a USER-KEYED namespace is a coincidence, not an address:
/// it is never a clone source, so a cas namespace asked for the same key
/// stores the bytes it was sent and keeps its own content.
#[test]
fn a_32_byte_user_key_is_never_a_clone_source() {
    let server = TestServer::new();
    let mut conn = server.connect();

    let _: String = redis::cmd("NSNEW").arg("named").query(&mut conn).unwrap();
    cas_namespace(&mut conn, "addressed");

    // A user-keyed namespace holding a 32-byte key whose value does NOT hash
    // to it -- which is entirely legal there.
    let value = b"the real content".to_vec();
    let key = address(&value);
    let _: String = redis::cmd("SELECT").arg("named").query(&mut conn).unwrap();
    assert_eq!(
        set(&mut conn, &key, b"something else entirely").unwrap(),
        "OK"
    );

    // The cas namespace must not take that as an address.
    let _: String = redis::cmd("SELECT")
        .arg("addressed")
        .query(&mut conn)
        .unwrap();
    assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");
    assert_eq!(
        get(&mut conn, &key).as_deref(),
        Some(&value[..]),
        "the value stored is the one that was verified, not the coincidence next door"
    );
    let checked: i64 = redis::cmd("CHECK").arg(&key).query(&mut conn).unwrap();
    assert_eq!(checked, 1);
}

/// cas + worm is the immutable archive the ADR promises: DEL is refused, so
/// an EXISTS answer can never be invalidated. A repeated SET of content that
/// is already there is still an acknowledgement -- it changes nothing, so
/// there is nothing for worm to protect.
#[test]
fn cas_plus_worm_refuses_deletes_and_still_acks_a_dedup_hit() {
    let server = TestServer::new();
    let mut conn = server.connect();
    select_cas(&mut conn, "archive");

    let value = b"kept forever".to_vec();
    let key = address(&value);
    assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");

    let _: String = redis::cmd("NSSET")
        .arg("archive")
        .arg("worm")
        .arg("1")
        .query(&mut conn)
        .expect("worm must be settable");

    let err = redis::cmd("DEL")
        .arg(&key)
        .query::<i64>(&mut conn)
        .expect_err("worm refuses deletes");
    assert!(format!("{err}").contains("worm"), "{err}");
    assert!(exists(&mut conn, &key), "the archive keeps what it has");

    // The probe-then-store workflow still works against a worm namespace.
    assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");

    // And new content can still be added: worm is write-once, not read-only.
    let other = b"added later".to_vec();
    let other_key = address(&other);
    assert_eq!(set(&mut conn, &other_key, &other).unwrap(), "OK");
    assert_eq!(get(&mut conn, &other_key).as_deref(), Some(&other[..]));
}

/// The reference counts a cross-namespace clone leaves behind, counted in the
/// store rather than inferred from the wire.
///
/// The wire says the clone is readable and that a DELETE next door does not
/// break it -- `a_hit_in_another_cas_namespace_is_cloned_by_reference` covers
/// that. What the wire cannot say is whether the count is EXACT, and an
/// off-by-one either way is a real fault: too low strands a live record when
/// the other namespace lets go, too high leaks the blocks forever. So the
/// daemon is stopped at every step and the numbers are read off the store.
#[test]
fn a_clone_holds_its_own_reference_and_the_counts_are_exact() {
    let value = big_value(0x2c);
    let key = address(&value);

    // Written once, in one namespace: one reference per block.
    let mut server = TestServer::new();
    let data_dir = server.data_dir().to_path_buf();
    {
        let mut conn = server.connect();
        cas_namespace(&mut conn, "first");
        cas_namespace(&mut conn, "second");
        let _: String = redis::cmd("SELECT").arg("first").query(&mut conn).unwrap();
        assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");
    }
    server.stop();

    let blocks = {
        let storage = open_store(&data_dir);
        let object = record(&storage, "first", &key).expect("the record must be there");
        assert!(!object.is_inlined(), "the value took the block path");
        let blocks = object.blocks().to_vec();
        assert!(blocks.len() > 1, "and needed more than one block");
        for block in &blocks {
            assert_eq!(rc_of(&storage, block), Some(1), "one holder, one reference");
        }
        assert_eq!(block_count(&storage), blocks.len());
        blocks
    };

    // The same address stored in the second namespace: no bytes move, and
    // every block gains exactly one reference.
    let mut running = TestServer::reopen(&data_dir);
    {
        let mut conn = running.connect();
        let _: String = redis::cmd("SELECT").arg("second").query(&mut conn).unwrap();
        assert_eq!(set(&mut conn, &key, &value).unwrap(), "OK");
    }
    running.stop();
    {
        let storage = open_store(&data_dir);
        for block in &blocks {
            assert_eq!(
                rc_of(&storage, block),
                Some(2),
                "each namespace accounts for itself"
            );
        }
        assert_eq!(
            block_count(&storage),
            blocks.len(),
            "the clone added a record, not a copy"
        );
        assert_eq!(block_files(&data_dir), blocks.len());
        let clone = record(&storage, "second", &key).expect("the clone must be there");
        assert_eq!(
            clone.blocks(),
            &blocks[..],
            "the clone names the same blocks, in the same order"
        );
    }

    // The source lets go: the count drops by one and nothing is freed.
    let mut running = TestServer::reopen(&data_dir);
    {
        let mut conn = running.connect();
        let _: String = redis::cmd("SELECT").arg("first").query(&mut conn).unwrap();
        let removed: i64 = redis::cmd("DEL").arg(&key).query(&mut conn).unwrap();
        assert_eq!(removed, 1);
    }
    running.stop();
    {
        let storage = open_store(&data_dir);
        assert!(record(&storage, "first", &key).is_none());
        for block in &blocks {
            assert_eq!(rc_of(&storage, block), Some(1), "the clone still holds one");
        }
        assert_eq!(block_files(&data_dir), blocks.len(), "no byte was freed");
    }

    // The last holder lets go: now everything goes.
    let mut running = TestServer::reopen(&data_dir);
    {
        let mut conn = running.connect();
        let _: String = redis::cmd("SELECT").arg("second").query(&mut conn).unwrap();
        assert_eq!(
            get(&mut conn, &key).as_deref(),
            Some(&value[..]),
            "still readable through the clone alone"
        );
        let removed: i64 = redis::cmd("DEL").arg(&key).query(&mut conn).unwrap();
        assert_eq!(removed, 1);
    }
    running.stop();
    {
        let storage = open_store(&data_dir);
        for block in &blocks {
            assert_eq!(rc_of(&storage, block), None, "the block record is gone");
        }
        assert_eq!(block_count(&storage), 0);
        assert_eq!(block_files(&data_dir), 0, "and so are the bytes");
    }
}
