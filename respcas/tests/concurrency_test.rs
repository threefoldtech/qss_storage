//! Several clients at once, on real connections.
//!
//! respcas serves every connection from its own tokio task over one shared
//! store, so anything a single connection cannot observe -- a reference count
//! that drifts, a value that comes back half-written, a namespace created
//! twice -- can only be reached from more than one at the same time. Every
//! test here starts its threads on a barrier so they actually overlap, and
//! asserts on the state once they have all finished rather than on anything
//! timed.

mod common;

use std::sync::Barrier;
use std::thread;

use common::{TestServer, block_count, block_files, open_store, rc_of, record};
use redis::Connection;

/// The address a client computes for itself.
fn address(value: &[u8]) -> Vec<u8> {
    blake3::hash(value).as_bytes().to_vec()
}

/// A value big enough to need blocks of its own, so the reference counts
/// under it are worth counting.
fn big_value(tag: u8) -> Vec<u8> {
    const BLOCK: usize = 1 << 20;
    let mut data = Vec::with_capacity(2 * BLOCK + 4096);
    for chunk in 0..2u8 {
        data.extend(std::iter::repeat_n(tag ^ chunk, BLOCK));
    }
    data.extend(std::iter::repeat_n(tag, 4096));
    data
}

fn cas_namespace(conn: &mut Connection, name: &str) {
    let _: String = redis::cmd("NSNEW").arg(name).query(conn).unwrap();
    let _: String = redis::cmd("NSSET")
        .arg(name)
        .arg("key_mode")
        .arg("cas")
        .query(conn)
        .unwrap();
}

fn select(conn: &mut Connection, name: &str) {
    let _: String = redis::cmd("SELECT").arg(name).query(conn).unwrap();
}

/// Every connection sends the same address and the same bytes. Whatever the
/// interleaving, the store ends up holding the content ONCE: one record, one
/// set of blocks, and exactly one reference on each of them.
///
/// The wire half of this is `cas_test::concurrent_sets_of_one_key_converge`.
/// What is added here is the count, which the wire cannot show: a store that
/// answered every read correctly while holding four references per block
/// would never free them.
#[test]
fn concurrent_writers_of_one_address_leave_exactly_one_reference() {
    const WRITERS: usize = 6;
    const ROUNDS: usize = 4;

    let value = big_value(0x91);
    let key = address(&value);

    let mut server = TestServer::new();
    let data_dir = server.data_dir().to_path_buf();
    {
        let mut setup = server.connect();
        cas_namespace(&mut setup, "blobs");
        select(&mut setup, "blobs");

        let barrier = Barrier::new(WRITERS);
        thread::scope(|scope| {
            for _ in 0..WRITERS {
                let mut conn = server.connect();
                let barrier = &barrier;
                let value = &value;
                let key = &key;
                scope.spawn(move || {
                    select(&mut conn, "blobs");
                    barrier.wait();
                    for _ in 0..ROUNDS {
                        let answer: String = redis::cmd("SET")
                            .arg(key)
                            .arg(value)
                            .query(&mut conn)
                            .expect("every writer must be acknowledged");
                        assert_eq!(answer, "OK");
                    }
                });
            }
        });

        // Quiesced, and readable through the wire before anything is opened.
        let read: Vec<u8> = redis::cmd("GET").arg(&key).query(&mut setup).unwrap();
        assert!(read == value, "one address, one content");
    }

    server.stop();
    let storage = open_store(&data_dir);
    let object = record(&storage, "blobs", &key).expect("the record must be there");
    assert!(!object.is_inlined());
    let blocks = object.blocks().to_vec();
    for block in &blocks {
        assert_eq!(
            rc_of(&storage, block),
            Some(1),
            "{WRITERS} writers x {ROUNDS} rounds is still one holder"
        );
    }
    assert_eq!(block_count(&storage), blocks.len(), "no block was doubled");
    assert_eq!(block_files(&data_dir), blocks.len());
}

/// One address, one connection writing it and another deleting it, over and
/// over, while a third reads.
///
/// The invariant is that a read is never SERVED a torn value: it gets the
/// whole thing, or nothing, or an error saying the content went away under
/// it. A short or mixed-up buffer would mean a reader can observe a value
/// mid-assembly, which no amount of retrying makes safe.
///
/// The end state is asserted rather than the timing: once the storm is over,
/// a write of the same content must leave a record whose bytes are all there
/// -- which is where a lost block, or a record left naming one, would show.
#[test]
fn a_set_and_delete_storm_never_serves_a_torn_value() {
    const ROUNDS: usize = 60;

    let value = big_value(0x4d);
    let key = address(&value);

    let mut server = TestServer::new();
    let data_dir = server.data_dir().to_path_buf();
    {
        let mut setup = server.connect();
        cas_namespace(&mut setup, "blobs");
        select(&mut setup, "blobs");

        let barrier = Barrier::new(3);
        let torn = thread::scope(|scope| {
            let writer = {
                let mut conn = server.connect();
                let (barrier, value, key) = (&barrier, &value, &key);
                scope.spawn(move || {
                    select(&mut conn, "blobs");
                    barrier.wait();
                    for _ in 0..ROUNDS {
                        let answer: String = redis::cmd("SET")
                            .arg(key)
                            .arg(value)
                            .query(&mut conn)
                            .expect("a write must be answered");
                        assert_eq!(answer, "OK");
                    }
                })
            };
            let deleter = {
                let mut conn = server.connect();
                let (barrier, key) = (&barrier, &key);
                scope.spawn(move || {
                    select(&mut conn, "blobs");
                    barrier.wait();
                    for _ in 0..ROUNDS {
                        let removed: i64 = redis::cmd("DEL")
                            .arg(key)
                            .query(&mut conn)
                            .expect("a delete must be answered");
                        assert!(removed == 0 || removed == 1, "DEL counts what it removed");
                    }
                })
            };
            let reader = {
                let mut conn = server.connect();
                let (barrier, value, key) = (&barrier, &value, &key);
                scope.spawn(move || {
                    select(&mut conn, "blobs");
                    barrier.wait();
                    let mut torn = 0usize;
                    for _ in 0..ROUNDS * 3 {
                        match redis::cmd("GET")
                            .arg(key)
                            .query::<Option<Vec<u8>>>(&mut conn)
                        {
                            // The whole value, or nothing at all.
                            Ok(Some(read)) if read == *value => {}
                            Ok(None) => {}
                            Ok(Some(read)) => {
                                torn += 1;
                                eprintln!(
                                    "a read was served {} bytes of a {} byte value",
                                    read.len(),
                                    value.len()
                                );
                            }
                            // The content went away between the record being
                            // resolved and its blocks being read. Refusing is
                            // allowed; answering with part of it is not.
                            Err(_) => {}
                        }
                    }
                    torn
                })
            };

            writer.join().expect("the writer must not panic");
            deleter.join().expect("the deleter must not panic");
            reader.join().expect("the reader must not panic")
        });
        assert_eq!(torn, 0, "no read may be served a partial value");

        // The storm is over. Whatever it left, the content stores and reads
        // back whole.
        let answer: String = redis::cmd("SET")
            .arg(&key)
            .arg(&value)
            .query(&mut setup)
            .expect("a write after the storm must be accepted");
        assert_eq!(answer, "OK");
        let read: Vec<u8> = redis::cmd("GET")
            .arg(&key)
            .query(&mut setup)
            .expect("and the value must be all there");
        assert!(read == value);
        let checked: i64 = redis::cmd("CHECK").arg(&key).query(&mut setup).unwrap();
        assert_eq!(checked, 1, "it still hashes to its key");
    }

    // And the store agrees: one holder per block, and no block left behind by
    // a delete that raced a write.
    server.stop();
    let storage = open_store(&data_dir);
    let object = record(&storage, "blobs", &key).expect("the record must be there");
    let blocks = object.blocks().to_vec();
    for block in &blocks {
        assert_eq!(rc_of(&storage, block), Some(1));
    }
    assert_eq!(
        block_count(&storage),
        blocks.len(),
        "a storm of {ROUNDS} writes and {ROUNDS} deletes left no orphan block"
    );
    assert_eq!(block_files(&data_dir), blocks.len());
}

/// Several connections create the same namespace at the same moment.
///
/// Exactly one of them may be told OK. NSNEW used to check for the name and
/// then insert it in two steps (`storage.rs::create_namespace`), so several
/// callers passed the check together and every one of them was told it had
/// created the namespace -- three or four of six, in practice -- while each
/// one's default record overwrote whatever the last had written. The claim is
/// one transaction now, and the losers are refused by name.
#[test]
fn concurrent_creates_of_one_namespace_leave_one_namespace() {
    const CREATORS: usize = 6;

    let server = TestServer::new();
    let mut setup = server.connect();

    let barrier = Barrier::new(CREATORS);
    let outcomes: Vec<Result<String, String>> = thread::scope(|scope| {
        let handles: Vec<_> = (0..CREATORS)
            .map(|_| {
                let mut conn = server.connect();
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    redis::cmd("NSNEW")
                        .arg("contested")
                        .query::<String>(&mut conn)
                        .map_err(|e| e.to_string())
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("no creator may panic"))
            .collect()
    });

    let created = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
    assert_eq!(
        created, 1,
        "one caller creates the namespace and the rest are refused: {outcomes:?}"
    );
    for outcome in &outcomes {
        match outcome {
            Ok(answer) => assert_eq!(answer, "OK"),
            Err(message) => assert!(
                message.contains("namespace contested already exists"),
                "the refusal a loser gets: {message}"
            ),
        }
    }

    // One namespace, once, and it works.
    let namespaces: Vec<String> = redis::cmd("NSLIST").query(&mut setup).unwrap();
    assert_eq!(
        namespaces
            .iter()
            .filter(|name| *name == "contested")
            .count(),
        1,
        "however many callers were told OK, the store holds it once: {namespaces:?}"
    );

    select(&mut setup, "contested");
    let _: String = redis::cmd("SET")
        .arg("proof")
        .arg("the namespace works")
        .query(&mut setup)
        .expect("the namespace that came out of the race is a real one");
    let read: String = redis::cmd("GET").arg("proof").query(&mut setup).unwrap();
    assert_eq!(read, "the namespace works");

    // Sequentially, with no race to lose: the second caller is refused, and
    // the namespace it collided with keeps its contents. The refusal says
    // what happened -- it used to say "Namespace not found" about a namespace
    // that was right there.
    let err = redis::cmd("NSNEW")
        .arg("contested")
        .query::<String>(&mut setup)
        .expect_err("a namespace cannot be created twice");
    assert!(
        format!("{err}").contains("namespace contested already exists"),
        "{err}"
    );
    let read: String = redis::cmd("GET").arg("proof").query(&mut setup).unwrap();
    assert_eq!(read, "the namespace works", "and nothing was reset");

    // And a namespace an NSSET has configured cannot be reset by a late
    // NSNEW: the claim is a claim, whenever it arrives.
    let _: String = redis::cmd("NSSET")
        .arg("contested")
        .arg("worm")
        .arg("1")
        .query(&mut setup)
        .expect("the namespace is configurable");
    assert!(
        redis::cmd("NSNEW")
            .arg("contested")
            .query::<String>(&mut setup)
            .is_err()
    );
    let info: String = redis::cmd("NSINFO")
        .arg("contested")
        .query(&mut setup)
        .unwrap();
    assert!(
        info.contains("worm: yes"),
        "a refused NSNEW writes no default record: {info}"
    );
}
