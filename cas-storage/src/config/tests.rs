use super::*;

/// Every key the schema has, so a field added without a parse arm shows up
/// here as a compile or assertion failure.
const FULL: &str = r#"
[store]
durability = "fsync"
inline_metadata_size = 4096
metadata_db = "fjall"
verify_on_read = true
stripe_count = 4096
max_blocks_per_commit = 32
group_commit = true
group_commit_window = "250us"

[store.hash]
algo = "blake3"
width = 16

[s3]
host = "0.0.0.0"
port = 9000
access_key = "AK"
secret_key = "SK"

[s3.metrics]
host = "127.0.0.1"
port = 9101

[multipart]
stale_ttl_days = 14

[resp]
host = "0.0.0.0"
port = 6380
data_dir = "/var/lib/respcas"
admin_password = "hunter2"
"#;

fn parse_str(text: &str) -> Result<QssStorageConfig, ConfigError> {
    parse(text, Path::new("qss_storage.toml"))
}

#[test]
fn full_file_parses() {
    let config = parse_str(FULL).expect("full file must parse");

    assert_eq!(config.store.durability, Some(Durability::Fsync));
    assert_eq!(config.store.inline_metadata_size, Some(4096));
    assert_eq!(config.store.metadata_db, Some(StorageEngine::Fjall));
    assert_eq!(config.store.verify_on_read, Some(true));
    assert_eq!(config.store.stripe_count, Some(4096));
    assert_eq!(config.store.max_blocks_per_commit, Some(32));
    assert_eq!(config.store.group_commit, Some(true));
    assert_eq!(
        config.store.group_commit_window.as_deref(),
        Some("250us"),
        "the window is kept as written and parsed once, at resolve"
    );
    assert_eq!(config.store.hash.algo.as_deref(), Some("blake3"));
    assert_eq!(config.store.hash.width, Some(16));
    assert_eq!(config.store.hash.hasher().unwrap(), Hasher::Blake3W16);

    let s3 = config.s3.expect("[s3] must parse");
    assert_eq!(s3.host.as_deref(), Some("0.0.0.0"));
    assert_eq!(s3.port, Some(9000));
    assert_eq!(s3.access_key.as_deref(), Some("AK"));
    assert_eq!(s3.secret_key.as_deref(), Some("SK"));
    let metrics = s3.metrics.expect("[s3.metrics] must parse");
    assert_eq!(metrics.host.as_deref(), Some("127.0.0.1"));
    assert_eq!(metrics.port, Some(9101));

    let multipart = config.multipart.expect("[multipart] must parse");
    assert_eq!(multipart.stale_ttl_days, Some(14));

    let resp = config.resp.expect("[resp] must parse");
    assert_eq!(resp.host.as_deref(), Some("0.0.0.0"));
    assert_eq!(resp.port, Some(6380));
    assert_eq!(resp.data_dir, Some(PathBuf::from("/var/lib/respcas")));
    assert_eq!(resp.admin_password.as_deref(), Some("hunter2"));
}

/// The shipped example is the documentation of this schema, so it has to
/// parse, and the values it presents as the defaults have to be the
/// defaults. This is the test that fails when a key is renamed here and
/// not there.
#[test]
fn the_example_file_parses_and_documents_the_defaults() {
    let text = include_str!("../../../qss_storage.toml.example");
    let config =
        parse(text, Path::new("qss_storage.toml.example")).expect("the shipped example must parse");

    assert_eq!(config.store.durability, Some(DEFAULT_DURABILITY));
    assert_eq!(config.store.metadata_db, Some(DEFAULT_METADATA_DB));
    assert_eq!(config.store.verify_on_read, Some(DEFAULT_VERIFY_ON_READ));
    assert_eq!(config.store.stripe_count, Some(DEFAULT_STRIPE_COUNT));
    assert_eq!(
        config.store.max_blocks_per_commit,
        Some(DEFAULT_MAX_BLOCKS_PER_COMMIT)
    );
    assert_eq!(config.store.group_commit, Some(DEFAULT_GROUP_COMMIT));
    assert_eq!(
        config
            .store
            .group_commit_window
            .as_deref()
            .map(parse_group_commit_window)
            .transpose()
            .expect("the example's window must parse"),
        Some(DEFAULT_GROUP_COMMIT_WINDOW)
    );
    assert_eq!(config.store.hash.algo.as_deref(), Some(DEFAULT_HASH_ALGO));
    assert_eq!(config.store.hash.width, Some(DEFAULT_HASH_WIDTH));

    let s3 = config.s3.expect("the example must show [s3]");
    assert_eq!(s3.host.as_deref(), Some(DEFAULT_S3_HOST));
    assert_eq!(s3.port, Some(DEFAULT_S3_PORT));
    let metrics = s3.metrics.expect("the example must show [s3.metrics]");
    assert_eq!(metrics.host.as_deref(), Some(DEFAULT_METRICS_HOST));
    assert_eq!(metrics.port, Some(DEFAULT_METRICS_PORT));

    let multipart = config.multipart.expect("the example must show [multipart]");
    assert_eq!(
        multipart.stale_ttl_days,
        Some(DEFAULT_MULTIPART_STALE_TTL_DAYS)
    );

    let resp = config.resp.expect("the example must show [resp]");
    assert_eq!(resp.host.as_deref(), Some(DEFAULT_RESP_HOST));
    assert_eq!(resp.port, Some(DEFAULT_RESP_PORT));
    assert_eq!(resp.data_dir, Some(PathBuf::from(DEFAULT_RESP_DATA_DIR)));

    // Commented out in the example, so the number in the comment is what
    // is checked -- the same way the other opt-in keys are documented.
    assert!(
        text.contains(&format!("#max_value_size = {DEFAULT_RESP_MAX_VALUE_SIZE}")),
        "the example must document the value cap default"
    );
}

/// The value cap is an opt-in key: absent means the built-in default,
/// and a file that sets it is read.
#[test]
fn the_value_cap_reads_from_the_resp_table() {
    let absent = parse_str("[resp]\nport = 6380\n").unwrap();
    assert_eq!(absent.resp.unwrap().max_value_size, None);

    let set = parse_str("[resp]\nmax_value_size = 1048576\n").unwrap();
    assert_eq!(set.resp.unwrap().max_value_size, Some(1024 * 1024));
}

#[test]
fn empty_file_is_the_default_config() {
    assert_eq!(parse_str("").unwrap(), QssStorageConfig::default());
    assert_eq!(
        QssStorageConfig::default()
            .store
            .hash
            .hasher()
            .expect("the default hash section must resolve"),
        Hasher::Blake3W32
    );
}

#[test]
fn absent_keys_stay_none() {
    let config = parse_str("[store]\nverify_on_read = true\n\n[resp]\nport = 6380\n").unwrap();

    // Set in the file.
    assert_eq!(config.store.verify_on_read, Some(true));
    assert_eq!(config.resp.as_ref().unwrap().port, Some(6380));

    // Absent from the file: None, not a default, so a CLI flag can win and
    // the built-in default applies when it does not.
    assert_eq!(config.store.durability, None);
    assert_eq!(config.store.metadata_db, None);
    assert_eq!(config.store.inline_metadata_size, None);
    assert_eq!(config.store.stripe_count, None);
    assert_eq!(config.store.max_blocks_per_commit, None);
    assert_eq!(config.store.group_commit, None);
    assert_eq!(config.store.group_commit_window, None);
    assert_eq!(config.store.hash.width, None);
    assert_eq!(config.resp.as_ref().unwrap().host, None);
    assert_eq!(config.resp.as_ref().unwrap().data_dir, None);
    assert!(config.s3.is_none());
    assert!(config.multipart.is_none());
}

#[test]
fn unknown_top_level_key_is_an_error() {
    let err = parse_str("[storage]\ndurability = \"fsync\"\n").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("storage"), "message must name the key: {msg}");
    assert!(
        msg.contains("qss_storage.toml"),
        "message must name the file: {msg}"
    );
}

#[test]
fn unknown_nested_key_is_an_error() {
    let err = parse_str("[store]\nverify_on_reads = true\n").unwrap_err();
    assert!(
        err.to_string().contains("verify_on_reads"),
        "message must name the key: {err}"
    );

    let err = parse_str("[s3.metrics]\nbind = \"127.0.0.1\"\n").unwrap_err();
    assert!(
        err.to_string().contains("bind"),
        "message must name the key: {err}"
    );

    let err = parse_str("[multipart]\nstale_ttl = 7\n").unwrap_err();
    assert!(
        err.to_string().contains("stale_ttl"),
        "message must name the key: {err}"
    );
}

/// Zero is a legal value, not a missing one: it is how an operator turns
/// the sweep off, and it must be distinguishable from an absent key (which
/// takes the 7 day default).
#[test]
fn a_zero_multipart_ttl_parses_as_a_value() {
    let config = parse_str("[multipart]\nstale_ttl_days = 0\n").unwrap();
    assert_eq!(config.multipart.unwrap().stale_ttl_days, Some(0));

    let config = parse_str("[multipart]\n").unwrap();
    assert_eq!(config.multipart.unwrap().stale_ttl_days, None);
}

#[test]
fn bad_durability_is_an_error() {
    let err = parse_str("[store]\ndurability = \"sync\"\n").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("unknown durability option: sync"), "{msg}");
    assert!(
        msg.contains("buffer") && msg.contains("fsync"),
        "message must list the two options: {msg}"
    );
}

/// The level ADR 0010 removed, with no alias. The refusal IS the
/// migration, so the message has to say which ADR took it and which two
/// names are left -- "unknown durability option" alone would send an
/// operator hunting for a typo they did not make.
#[test]
fn removed_fdatasync_level_is_refused_with_the_two_that_remain() {
    let err = parse_str("[store]\ndurability = \"fdatasync\"\n").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("removed"), "message must say removed: {msg}");
    assert!(msg.contains("0010"), "message must name the ADR: {msg}");
    assert!(
        msg.contains("fsync") && msg.contains("buffer"),
        "message must name the two valid levels: {msg}"
    );
}

#[test]
fn bad_metadata_db_is_an_error() {
    let err = parse_str("[store]\nmetadata_db = \"sled\"\n").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("unknown storage engine: sled"), "{msg}");
    assert!(
        msg.contains("fjall"),
        "message must list the options: {msg}"
    );
}

/// The backend was removed by ADR 0007; the config value must fail loudly
/// and the message must carry the migration path, not just "unknown".
#[test]
fn removed_fjall_notx_is_rejected_with_the_migration_path() {
    let err = parse_str("[store]\nmetadata_db = \"fjall_notx\"\n").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("removed"), "message must say removed: {msg}");
    assert!(
        msg.contains("durability") && msg.contains("buffer"),
        "message must name the migration path: {msg}"
    );
}

/// The two ends of the usable range are refused at parse, next to the file
/// that holds them. Both failures are silent otherwise: zero collapses to
/// one global lock and anything larger than the two-byte index is
/// allocated and never taken, so an operator gets neither what they asked
/// for nor a complaint.
#[test]
fn a_stripe_count_outside_the_usable_range_is_an_error() {
    let err = parse_str("[store]\nstripe_count = 0\n").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains('0'), "message must name the value: {msg}");
    assert!(
        msg.contains(&MAX_STRIPE_COUNT.to_string()),
        "message must name the ceiling: {msg}"
    );

    let too_many = MAX_STRIPE_COUNT + 1;
    let err = parse_str(&format!("[store]\nstripe_count = {too_many}\n")).unwrap_err();
    assert!(
        err.to_string().contains(&too_many.to_string()),
        "message must name the value: {err}"
    );

    // The boundaries themselves are legal.
    for count in [1, DEFAULT_STRIPE_COUNT, MAX_STRIPE_COUNT] {
        let config = parse_str(&format!("[store]\nstripe_count = {count}\n"))
            .unwrap_or_else(|e| panic!("{count} must be accepted: {e}"));
        assert_eq!(config.store.stripe_count, Some(count));
    }
}

/// Zero is the one batch cap that cannot work; 1 is legal and means the
/// pre-ADR-0010 cadence, one commit per block.
#[test]
fn a_zero_batch_cap_is_an_error_and_one_is_not() {
    let err = parse_str("[store]\nmax_blocks_per_commit = 0\n").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains('0'), "message must name the value: {msg}");
    assert!(
        msg.contains("max_blocks_per_commit"),
        "message must name the setting: {msg}"
    );

    for cap in [1, 2, DEFAULT_MAX_BLOCKS_PER_COMMIT, 4096] {
        let config = parse_str(&format!("[store]\nmax_blocks_per_commit = {cap}\n"))
            .unwrap_or_else(|e| panic!("{cap} must be accepted: {e}"));
        assert_eq!(config.store.max_blocks_per_commit, Some(cap));
    }
}

/// The configured default and the one the store actually uses are one
/// value, not two that happen to match today.
#[test]
fn the_default_batch_cap_is_the_write_paths_own() {
    assert_eq!(
        DEFAULT_MAX_BLOCKS_PER_COMMIT,
        crate::cas::write_path::DEFAULT_MAX_BLOCKS_PER_COMMIT
    );
    validate_max_blocks_per_commit(DEFAULT_MAX_BLOCKS_PER_COMMIT)
        .expect("the built-in default must itself be a legal value");
}

/// The window is a duration with a unit, refused at parse time when it is
/// not one -- next to the file that holds it, as every other store knob
/// is.
#[test]
fn a_group_commit_window_that_is_not_a_duration_is_an_error() {
    let err = parse_str("[store]\ngroup_commit_window = \"soon\"\n").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("soon"), "message must name the value: {msg}");
    assert!(
        msg.contains("group_commit_window"),
        "message must name the setting: {msg}"
    );
    assert!(
        msg.contains("0ms"),
        "message must show a value that works: {msg}"
    );
}

/// A bare number is refused rather than guessed at. `"1"` could be a
/// second or a microsecond and the difference is six orders of magnitude
/// of ack latency, so the parser makes the operator say which.
#[test]
fn a_unitless_window_is_refused() {
    assert!(parse_str("[store]\ngroup_commit_window = \"1\"\n").is_err());
}

/// Zero is a value, not an absence: it is how the timer is turned off,
/// and it must be spellable.
#[test]
fn the_window_spellings_that_must_work() {
    for (text, expected) in [
        ("0ms", std::time::Duration::ZERO),
        ("0s", std::time::Duration::ZERO),
        ("250us", std::time::Duration::from_micros(250)),
        ("2ms", std::time::Duration::from_millis(2)),
        ("1s", std::time::Duration::from_secs(1)),
    ] {
        let config = parse_str(&format!("[store]\ngroup_commit_window = \"{text}\"\n"))
            .unwrap_or_else(|e| panic!("{text} must parse: {e}"));
        assert_eq!(
            parse_group_commit_window(config.store.group_commit_window.as_deref().unwrap())
                .unwrap(),
            expected,
            "{text}"
        );
    }
}

/// Group commit is off unless the file says otherwise, and `false` is
/// distinguishable from absent -- an operator who writes it out
/// explicitly gets the same behaviour, not a different code path.
#[test]
fn group_commit_is_off_by_default_and_false_is_a_value() {
    // The ADR 0011 default is off, with no timer.
    const { assert!(!DEFAULT_GROUP_COMMIT) };
    assert_eq!(DEFAULT_GROUP_COMMIT_WINDOW, std::time::Duration::ZERO);

    let config = parse_str("[store]\ngroup_commit = false\n").unwrap();
    assert_eq!(config.store.group_commit, Some(false));
    let config = parse_str("[store]\n").unwrap();
    assert_eq!(config.store.group_commit, None);
}

/// The configured default and the one the store actually uses are one
/// value, not two that happen to match today.
#[test]
fn the_default_stripe_count_is_the_stores_own() {
    assert_eq!(
        DEFAULT_STRIPE_COUNT,
        crate::cas::stripes::DEFAULT_STRIPE_COUNT
    );
    validate_stripe_count(DEFAULT_STRIPE_COUNT)
        .expect("the built-in default must itself be a legal value");
}

#[test]
fn bad_hash_algo_is_an_error() {
    let err = parse_str("[store.hash]\nalgo = \"md5\"\n").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("md5"),
        "message must name the algorithm: {msg}"
    );
    assert!(
        msg.contains("blake3"),
        "message must name the option: {msg}"
    );
}

#[test]
fn bad_hash_width_is_an_error() {
    let err = parse_str("[store.hash]\nwidth = 24\n").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("24"), "message must name the width: {msg}");
    assert!(
        msg.contains("16 or 32"),
        "message must list the widths: {msg}"
    );
}

#[test]
fn parse_error_carries_file_and_position() {
    let err = parse_str("[store\ndurability = \"fsync\"\n").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("qss_storage.toml"),
        "message must name the file: {msg}"
    );
    // toml renders a span; the line number is what an operator needs.
    assert!(
        msg.contains("1") || msg.contains("line"),
        "message must locate the error: {msg}"
    );
}

#[test]
fn explicit_path_wins_over_the_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let explicit = dir.path().join("custom.toml");
    let cwd_file = dir.path().join(CONFIG_FILE_NAME);
    std::fs::write(&explicit, "").unwrap();
    std::fs::write(&cwd_file, "").unwrap();

    let picked = select_path(Some(&explicit), std::slice::from_ref(&cwd_file))
        .unwrap()
        .unwrap();
    assert_eq!(picked, explicit);
}

#[test]
fn missing_explicit_path_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("nope.toml");

    let err = select_path(Some(&missing), &[]).unwrap_err();
    assert!(matches!(err, ConfigError::NotFound(_)));
    assert!(
        err.to_string().contains("nope.toml"),
        "message must name the path: {err}"
    );
}

#[test]
fn first_existing_candidate_wins() {
    let dir = tempfile::tempdir().unwrap();
    let cwd_file = dir.path().join(CONFIG_FILE_NAME);
    let system_file = dir.path().join("etc-qss_storage.toml");
    std::fs::write(&system_file, "").unwrap();

    // Only the system file exists.
    let candidates = vec![cwd_file.clone(), system_file.clone()];
    assert_eq!(
        select_path(None, &candidates).unwrap(),
        Some(system_file.clone())
    );

    // Once the working-directory file exists it takes precedence.
    std::fs::write(&cwd_file, "").unwrap();
    assert_eq!(select_path(None, &candidates).unwrap(), Some(cwd_file));
}

#[test]
fn no_candidate_means_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let candidates = vec![
        dir.path().join(CONFIG_FILE_NAME),
        dir.path().join("etc-qss_storage.toml"),
    ];
    assert_eq!(select_path(None, &candidates).unwrap(), None);
}

#[test]
fn load_reads_the_explicit_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("custom.toml");
    std::fs::write(&path, "[store]\ndurability = \"buffer\"\n").unwrap();

    let (config, source) = load(Some(&path)).unwrap();
    assert_eq!(config.store.durability, Some(Durability::Buffer));
    assert_eq!(source, Some(path));
}

#[test]
fn durability_and_engine_round_trip_through_display() {
    for durability in [Durability::Buffer, Durability::Fsync] {
        let text = format!("[store]\ndurability = \"{durability}\"\n");
        assert_eq!(parse_str(&text).unwrap().store.durability, Some(durability));
    }
    // One engine since ADR 0007 removed the other; make this a loop
    // again when a second variant lands.
    let engine = StorageEngine::Fjall;
    let text = format!("[store]\nmetadata_db = \"{engine}\"\n");
    assert_eq!(parse_str(&text).unwrap().store.metadata_db, Some(engine));
}
