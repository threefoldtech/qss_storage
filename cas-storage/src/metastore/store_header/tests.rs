use super::*;
use crate::metastore::{FjallStore, MetaError, MetaStore, Store};
use std::path::{Path, PathBuf};
use tempfile::{TempDir, tempdir};

/// A header created with the default spec at a pinned timestamp and no
/// store id, byte for byte -- the shape every pre-ADR-0012 store has on
/// disk. Changing this vector changes the on-disk format.
///
/// magic "QSST" | version 3 | algo 1 (blake3) | width 32 |
/// created_at 0x0000000068000001 | 16 zero bytes (store id absent)
const GOLDEN: [u8; STORE_HEADER_SIZE] = [
    0x51, 0x53, 0x53, 0x54, // "QSST"
    0x03, 0x00, // version 3 (ADR 0005 block record flags byte)
    0x01, // algo: blake3
    0x20, // width: 32
    0x01, 0x00, 0x00, 0x68, 0x00, 0x00, 0x00, 0x00, // created_at
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // store id: absent
];

const GOLDEN_CREATED_AT: u64 = 0x0000_0000_6800_0001;

#[test]
fn golden_vector() {
    let header = StoreHeader::create_with(HeaderSpec::default(), GOLDEN_CREATED_AT, None).unwrap();
    assert_eq!(header.to_bytes(), GOLDEN);

    let decoded = StoreHeader::from_bytes(&GOLDEN).unwrap();
    assert_eq!(decoded.version(), STORE_HEADER_VERSION);
    assert_eq!(decoded.hash_algo(), 1);
    assert_eq!(decoded.hash_width(), 32);
    assert_eq!(decoded.created_at(), GOLDEN_CREATED_AT);
    assert_eq!(decoded.hasher(), Hasher::Blake3W32);
    assert_eq!(decoded.store_id(), None, "an all-zero id is no id");
    assert_eq!(decoded, header);
}

/// The other half of the vector: the same header with an id in it. The
/// id occupies the last 16 bytes and nothing else moves.
#[test]
fn golden_vector_with_a_store_id() {
    let id = StoreId::from_bytes([
        0x0f, 0x1e, 0x2d, 0x3c, 0x4b, 0x5a, 0x69, 0x78, 0x87, 0x96, 0xa5, 0xb4, 0xc3, 0xd2, 0xe1,
        0xf0,
    ])
    .unwrap();
    let header =
        StoreHeader::create_with(HeaderSpec::default(), GOLDEN_CREATED_AT, Some(id)).unwrap();

    let mut expected = GOLDEN;
    expected[16..].copy_from_slice(id.as_bytes());
    assert_eq!(header.to_bytes(), expected);
    assert_eq!(header.store_id(), Some(id));
    assert_eq!(id.to_hex(), "0f1e2d3c4b5a69788796a5b4c3d2e1f0");
    assert_eq!(StoreId::parse_hex(&format!("{id}\n")), Some(id));
    assert_eq!(StoreHeader::from_bytes(&expected).unwrap(), header);
}

/// The absent pattern is not a value, and neither is a truncated or
/// non-hex marker: all three are "this root claims nothing".
#[test]
fn store_ids_reject_the_absent_pattern_and_junk() {
    assert_eq!(StoreId::from_bytes([0u8; STORE_ID_SIZE]), None);
    assert_eq!(StoreId::parse_hex(&"0".repeat(32)), None);
    assert_eq!(StoreId::parse_hex(""), None);
    assert_eq!(StoreId::parse_hex("deadbeef"), None);
    assert_eq!(StoreId::parse_hex(&"z".repeat(32)), None);
    assert_ne!(StoreId::generate(), StoreId::generate());
    let id = StoreId::generate();
    assert_eq!(StoreId::parse_hex(&id.to_hex()), Some(id));
}

/// A created store gets an id; adoption is only for stores that predate
/// the field.
#[test]
fn created_headers_carry_a_fresh_id() {
    let first = StoreHeader::create(HeaderSpec::default()).unwrap();
    let second = StoreHeader::create(HeaderSpec::default()).unwrap();
    assert!(first.store_id().is_some());
    assert_ne!(first.store_id(), second.store_id());
}

#[test]
fn round_trips_both_widths() {
    for hasher in [Hasher::Blake3W16, Hasher::Blake3W32] {
        let header = StoreHeader::create_at(hasher.into(), 42).unwrap();
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), STORE_HEADER_SIZE);
        assert_eq!(StoreHeader::from_bytes(&bytes).unwrap(), header);
        assert_eq!(header.hasher(), hasher);
    }
}

/// Whatever is in the id bytes round-trips: a build that only passes a
/// header through must neither reject an id it did not mint nor drop it.
#[test]
fn store_id_bytes_round_trip_untouched() {
    let mut raw = GOLDEN;
    raw[16..].copy_from_slice(&[0xabu8; STORE_ID_SIZE]);

    let header = StoreHeader::from_bytes(&raw).expect("any id pattern must be accepted");
    assert_eq!(header.to_bytes(), raw);
    assert_eq!(header.store_id().unwrap().as_bytes(), &[0xabu8; 16]);
}

/// Adoption changes the id and nothing else.
#[test]
fn with_store_id_touches_only_the_id() {
    let before = StoreHeader::from_bytes(&GOLDEN).unwrap();
    let id = StoreId::generate();
    let after = before.with_store_id(id);

    assert_eq!(after.store_id(), Some(id));
    assert_eq!(after.version(), before.version());
    assert_eq!(after.hasher(), before.hasher());
    assert_eq!(after.created_at(), before.created_at());
    assert_eq!(&after.to_bytes()[..16], &before.to_bytes()[..16]);
}

#[test]
fn rejects_bad_magic() {
    let mut raw = GOLDEN;
    raw[..4].copy_from_slice(b"JUNK");
    let err = StoreHeader::from_bytes(&raw).unwrap_err();
    assert_eq!(err, StoreHeaderError::BadMagic(*b"JUNK"));
    let msg = err.to_string();
    assert!(msg.contains("0x4a554e4b"), "{msg}");
    assert!(msg.contains("\"JUNK\""), "{msg}");
    assert!(msg.contains("expected \"QSST\""), "{msg}");
}

#[test]
fn unprintable_magic_is_still_named() {
    let mut raw = GOLDEN;
    raw[..4].copy_from_slice(&[0x00, 0xff, 0x41, 0x0a]);
    let msg = StoreHeader::from_bytes(&raw).unwrap_err().to_string();
    assert!(msg.contains("0x00ff410a"), "{msg}");
    assert!(msg.contains("\"..A.\""), "{msg}");
}

#[test]
fn rejects_unsupported_version() {
    let mut raw = GOLDEN;
    raw[4..6].copy_from_slice(&5u16.to_le_bytes());
    let err = StoreHeader::from_bytes(&raw).unwrap_err();
    assert_eq!(err, StoreHeaderError::UnsupportedVersion(5));
    assert!(
        err.to_string()
            .contains("unsupported QSST store format version 5"),
        "{err}"
    );
}

/// A store that has grown a Cas namespace (ADR 0014) says v4 and is
/// opened by this build; nothing else about the record moves.
#[test]
fn the_cas_namespace_version_is_opened_and_changes_nothing_else() {
    let mut raw = GOLDEN;
    raw[4..6].copy_from_slice(&STORE_HEADER_VERSION_CAS_NAMESPACE.to_le_bytes());

    let header = StoreHeader::from_bytes(&raw).expect("v4 is a version this build opens");
    assert_eq!(header.version(), STORE_HEADER_VERSION_CAS_NAMESPACE);
    assert_eq!(header.hasher(), Hasher::Blake3W32);
    assert_eq!(header.created_at(), GOLDEN_CREATED_AT);
    assert_eq!(header.to_bytes(), raw, "the raise costs no other byte");
}

/// The raise is one-way, idempotent, and refuses a version this build
/// could not itself open.
#[test]
fn raising_the_version_is_one_way_and_idempotent() {
    let dir = tempdir().unwrap();
    let store_dir = dir.path().to_path_buf();
    let db_path = store_dir.join("db");
    let (store, created) =
        MetaStore::open_or_create(db_path.clone(), Some(1), HeaderSpec::default(), fjall)
            .expect("fresh directory must be created");
    assert_eq!(created.version(), STORE_HEADER_VERSION);

    let raised = raise_version(
        &*store.get_underlying_store(),
        &db_path,
        &store_dir,
        STORE_HEADER_VERSION_CAS_NAMESPACE,
    )
    .unwrap();
    assert_eq!(raised.version(), STORE_HEADER_VERSION_CAS_NAMESPACE);
    assert_eq!(raised.store_id(), created.store_id(), "identity is kept");

    // On disk, and in the sidecar the store keeps beside its database.
    let on_disk = read_header(&*store.get_underlying_store(), &db_path)
        .unwrap()
        .unwrap();
    assert_eq!(on_disk, raised);
    let sidecar = std::fs::read(store_dir.join(STORE_HEADER_SIDECAR)).unwrap();
    assert_eq!(StoreHeader::from_bytes(&sidecar).unwrap(), raised);

    // Idempotent, and never a downgrade.
    let again = raise_version(
        &*store.get_underlying_store(),
        &db_path,
        &store_dir,
        STORE_HEADER_VERSION_CAS_NAMESPACE,
    )
    .unwrap();
    assert_eq!(again, raised);
    let down = raise_version(
        &*store.get_underlying_store(),
        &db_path,
        &store_dir,
        STORE_HEADER_VERSION,
    )
    .unwrap();
    assert_eq!(down, raised, "a lower version leaves the header alone");

    // A version this build cannot open is not one it may write.
    let err = raise_version(&*store.get_underlying_store(), &db_path, &store_dir, 99).unwrap_err();
    assert!(err.to_string().contains("99"), "{err}");
}

/// A store whose database IS its own directory -- respcas before ADR
/// 0014 -- keeps its sidecar inside itself, and writes nothing above it.
///
/// The layout the derived-from-the-database-path rule got wrong: it put
/// the raised copy in the store's PARENT, which is not part of any store,
/// and left the copy an operator would actually find stale at the old
/// version.
#[test]
fn a_store_that_is_its_own_database_keeps_its_sidecar_inside_itself() {
    let outer = tempdir().unwrap();
    let store_dir = outer.path().join("store");
    std::fs::create_dir_all(&store_dir).unwrap();

    // The database directly in the store directory, which is what the
    // pre-0014 layout is. Its creation is the OLD build's, and that build
    // put the sidecar above the store; the store as found in the field is
    // the one without it, so that is the store this test raises.
    let (store, created) =
        MetaStore::open_or_create(store_dir.clone(), Some(1), HeaderSpec::default(), fjall)
            .expect("fresh directory must be created");
    std::fs::remove_file(outer.path().join(STORE_HEADER_SIDECAR)).ok();

    let raised = raise_version(
        &*store.get_underlying_store(),
        &store_dir,
        &store_dir,
        STORE_HEADER_VERSION_CAS_NAMESPACE,
    )
    .unwrap();
    assert_eq!(raised.version(), STORE_HEADER_VERSION_CAS_NAMESPACE);
    assert_ne!(raised.version(), created.version());

    let sidecar = std::fs::read(store_dir.join(STORE_HEADER_SIDECAR))
        .expect("the sidecar is inside the store");
    assert_eq!(StoreHeader::from_bytes(&sidecar).unwrap(), raised);
    assert!(
        !outer.path().join(STORE_HEADER_SIDECAR).exists(),
        "and nothing was written outside it"
    );
}

/// The migration gate for ADR 0005's block record change: a v2 store
/// must be refused at open, not misread.
#[test]
fn rejects_the_previous_version() {
    let mut raw = GOLDEN;
    raw[4..6].copy_from_slice(&2u16.to_le_bytes());
    let err = StoreHeader::from_bytes(&raw).unwrap_err();
    assert_eq!(err, StoreHeaderError::UnsupportedVersion(2));
}

#[test]
fn rejects_unknown_algo_and_width() {
    let mut raw = GOLDEN;
    raw[6] = 9;
    let err = StoreHeader::from_bytes(&raw).unwrap_err();
    assert_eq!(err, StoreHeaderError::Hash(HasherError::UnknownAlgo(9)));
    assert!(
        err.to_string()
            .contains("unknown block hash algorithm id 9"),
        "{err}"
    );

    let mut raw = GOLDEN;
    raw[7] = 17;
    let err = StoreHeader::from_bytes(&raw).unwrap_err();
    assert_eq!(
        err,
        StoreHeaderError::Hash(HasherError::UnsupportedWidth(17))
    );
    assert!(
        err.to_string().contains("unsupported block hash width 17"),
        "{err}"
    );
}

#[test]
fn rejects_wrong_length() {
    let short = StoreHeader::from_bytes(&GOLDEN[..31]).unwrap_err();
    assert!(
        matches!(
            short,
            StoreHeaderError::Malformed(FsError::Truncated { .. })
        ),
        "{short:?}"
    );

    let mut long = GOLDEN.to_vec();
    long.push(0);
    let long = StoreHeader::from_bytes(&long).unwrap_err();
    assert!(
        matches!(
            long,
            StoreHeaderError::Malformed(FsError::TrailingBytes { .. })
        ),
        "{long:?}"
    );
}

// ---- store-level tests ----

fn fjall(path: PathBuf) -> Result<FjallStore, MetaError> {
    FjallStore::new(path, Some(1), None)
}

/// Creating a store writes a header; reopening the same directory reads
/// exactly that header back.
fn create_then_reopen<S: Store + 'static>(build: impl Fn(PathBuf) -> Result<S, MetaError>) {
    let dir = tempdir().unwrap();
    let db = dir.path().join("db");

    let created = {
        let (_store, header) =
            MetaStore::open_or_create(db.clone(), Some(1), HeaderSpec::default(), &build)
                .expect("fresh directory must be created");
        header
    };
    assert_eq!(created.hasher(), Hasher::Blake3W32);

    let (_store, reopened) =
        MetaStore::open_or_create(db.clone(), Some(1), HeaderSpec::default(), &build)
            .expect("a store we just created must reopen");
    assert_eq!(reopened, created);
}

#[test]
fn create_then_reopen_fjall() {
    create_then_reopen(fjall);
}

/// The width is taken from the header on open, not from the spec the
/// caller happens to pass.
#[test]
fn header_wins_over_spec_on_open() {
    let dir = tempdir().unwrap();
    let db = dir.path().join("db");

    // The first store has to be dropped before the second opens: fjall
    // holds a lock on the directory for as long as the database lives.
    let created = {
        let (_store, created) =
            MetaStore::open_or_create(db.clone(), Some(1), Hasher::Blake3W16.into(), fjall)
                .unwrap();
        created
    };
    assert_eq!(created.hasher(), Hasher::Blake3W16);

    let (_store, reopened) =
        MetaStore::open_or_create(db.clone(), Some(1), Hasher::Blake3W32.into(), fjall).unwrap();
    assert_eq!(reopened.hasher(), Hasher::Blake3W16);
}

#[test]
fn creation_writes_the_sidecar() {
    let dir = tempdir().unwrap();
    let db = dir.path().join("db");
    assert!(!db.exists(), "the create path starts from a missing dir");

    let (_store, header) =
        MetaStore::open_or_create(db.clone(), Some(1), HeaderSpec::default(), fjall).unwrap();

    let sidecar = dir.path().join(STORE_HEADER_SIDECAR);
    let bytes = std::fs::read(&sidecar).expect("sidecar must exist next to the db dir");
    assert_eq!(bytes, header.to_bytes());
}

/// Adoption is durable on both copies: the record a reopen reads and the
/// sidecar a manual recovery reads say the same id.
#[test]
fn adoption_writes_the_header_record_and_the_sidecar() {
    let dir = tempdir().unwrap();
    let db = dir.path().join("db");
    let adopted = StoreId::generate();

    let created = {
        let (meta, header) =
            MetaStore::open_or_create(db.clone(), Some(1), HeaderSpec::default(), fjall).unwrap();
        assert!(header.store_id().is_some(), "a new store mints its own");
        adopt_store_id(&*meta.get_underlying_store(), dir.path(), header, adopted).unwrap()
    };
    assert_eq!(created.store_id(), Some(adopted));

    let (_meta, reopened) =
        MetaStore::open_or_create(db.clone(), Some(1), HeaderSpec::default(), fjall).unwrap();
    assert_eq!(reopened.store_id(), Some(adopted));

    let sidecar = std::fs::read(dir.path().join(STORE_HEADER_SIDECAR)).unwrap();
    assert_eq!(sidecar, created.to_bytes());
}

/// Builds a store the raw way (no header), leaving a non-empty db
/// directory behind, and returns the path it lives at.
fn unheadered_store(dir: &TempDir) -> PathBuf {
    let db = dir.path().join("db");
    let store = fjall(db.clone()).unwrap();
    // Give the store some content, so that it is a real pre-QSST store and
    // not just an empty directory.
    store
        .tree_open("bucket")
        .unwrap()
        .insert(b"key", b"value".to_vec())
        .unwrap();
    drop(store);
    db
}

#[test]
fn refuses_a_store_that_predates_the_header() {
    let dir = tempdir().unwrap();
    let db = unheadered_store(&dir);

    let err = MetaStore::open_or_create(db, Some(1), HeaderSpec::default(), fjall).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("store predates the QSST format; no migration exists"),
        "{msg}"
    );
}

/// Overwrites the header record of an existing store with `raw`, the way a
/// corrupted or foreign header would look on disk.
fn doctor_header(db: &Path, raw: Vec<u8>) {
    let store = fjall(db.to_path_buf()).unwrap();
    store
        .tree_open(STORE_HEADER_TREE)
        .unwrap()
        .insert(STORE_HEADER_KEY, raw)
        .unwrap();
    drop(store);
}

/// Creates a healthy store, doctors its header with `raw`, and returns the
/// error text produced by reopening it.
fn refusal_for(raw: Vec<u8>) -> String {
    let dir = tempdir().unwrap();
    let db = dir.path().join("db");
    {
        let _ =
            MetaStore::open_or_create(db.clone(), Some(1), HeaderSpec::default(), fjall).unwrap();
    }
    doctor_header(&db, raw);

    MetaStore::open_or_create(db, Some(1), HeaderSpec::default(), fjall)
        .expect_err("a doctored header must be refused")
        .to_string()
}

#[test]
fn refuses_a_doctored_magic() {
    let mut raw = GOLDEN;
    raw[..4].copy_from_slice(b"SLED");
    let msg = refusal_for(raw.to_vec());
    assert!(msg.contains("not a QSST store"), "{msg}");
    assert!(msg.contains("\"SLED\""), "{msg}");
}

#[test]
fn refuses_a_doctored_version() {
    let mut raw = GOLDEN;
    raw[4..6].copy_from_slice(&9u16.to_le_bytes());
    let msg = refusal_for(raw.to_vec());
    assert!(
        msg.contains("unsupported QSST store format version 9"),
        "{msg}"
    );
}

/// The other side of the ADR 0014 gate, at store level: a store whose
/// header was raised to the Cas-namespace version still opens here, and
/// its version is not quietly written back down.
#[test]
fn a_cas_namespace_store_reopens_at_its_raised_version() {
    let dir = tempdir().unwrap();
    let db = dir.path().join("db");
    {
        let _ =
            MetaStore::open_or_create(db.clone(), Some(1), HeaderSpec::default(), fjall).unwrap();
    }
    let mut raw = GOLDEN;
    raw[4..6].copy_from_slice(&STORE_HEADER_VERSION_CAS_NAMESPACE.to_le_bytes());
    doctor_header(&db, raw.to_vec());

    let (_store, header) = MetaStore::open_or_create(db, Some(1), HeaderSpec::default(), fjall)
        .expect("a raised store must open");
    assert_eq!(header.version(), STORE_HEADER_VERSION_CAS_NAMESPACE);
}

#[test]
fn refuses_a_doctored_algo() {
    let mut raw = GOLDEN;
    raw[6] = 9;
    let msg = refusal_for(raw.to_vec());
    assert!(msg.contains("unknown block hash algorithm id 9"), "{msg}");
}

#[test]
fn refuses_a_doctored_width() {
    let mut raw = GOLDEN;
    raw[7] = 17;
    let msg = refusal_for(raw.to_vec());
    assert!(msg.contains("unsupported block hash width 17"), "{msg}");
}

#[test]
fn refuses_a_truncated_header_record() {
    let msg = refusal_for(GOLDEN[..16].to_vec());
    assert!(msg.contains("malformed QSST store header"), "{msg}");
}

/// Every refusal names the store it is about, so an operator running
/// several stores knows which one to look at.
#[test]
fn refusals_name_the_store_path() {
    let dir = tempdir().unwrap();
    let db = unheadered_store(&dir);
    let msg = MetaStore::open_or_create(db.clone(), Some(1), HeaderSpec::default(), fjall)
        .unwrap_err()
        .to_string();
    assert!(msg.contains(&db.display().to_string()), "{msg}");
}
