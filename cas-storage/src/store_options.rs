//! Store settings resolved from CLI flags, the config file, and the built-in
//! defaults -- in that order of precedence.
//!
//! Every tool that opens a store has to agree on how: one that opens the store
//! with a different backend or a different inline threshold than the server
//! wrote it with reads the wrong thing. So the resolution lives here once, in
//! the library, and every binary -- s3cas's subcommands and fsck alike --
//! calls it. The flag definitions themselves stay with each binary's clap
//! parser; only the merge is shared.

use crate::config::{
    ConfigError, DEFAULT_DURABILITY, DEFAULT_METADATA_DB, DEFAULT_VERIFY_ON_READ, StoreConfig,
    validate_stripe_count,
};
use crate::{Durability, Hasher, HeaderSpec, StorageEngine};

/// How this process opens (and, if it does not exist yet, creates) its store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreOptions {
    /// Metadata database backend.
    pub metadata_db: StorageEngine,
    /// Write durability of the metadata database.
    pub durability: Durability,
    /// Inline threshold, `None` for the backend default.
    pub inline_metadata_size: Option<usize>,
    /// Re-hash blocks on read.
    pub verify_on_read: bool,
    /// Block hash for stores created now. Ignored when the store already
    /// exists: its header wins.
    pub hasher: Hasher,
    /// Per-block lock stripes, `None` for the built-in default.
    ///
    /// Unlike `hasher`, this is not a property of the store on disk -- it is
    /// how THIS process serializes its own block writers, so it applies on
    /// every open and two processes may legitimately differ.
    pub stripe_count: Option<usize>,
}

impl StoreOptions {
    /// Merges CLI flags over the `[store]` table over the built-in defaults.
    ///
    /// Each `Option` argument is the CLI flag: `Some` means the operator
    /// passed it, which is why the clap fields carry no `default_value` --
    /// clap cannot otherwise tell a passed flag from a default one, and a
    /// default that looks like a passed flag would always beat the config
    /// file.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] if `store.hash` does not name a hash this build has, or
    /// if the merged stripe count is outside the usable range. The stripe
    /// count is validated here and not only at parse time because the CLI flag
    /// can supply one the config file never saw.
    pub fn resolve(
        metadata_db: Option<StorageEngine>,
        durability: Option<Durability>,
        inline_metadata_size: Option<usize>,
        stripe_count: Option<usize>,
        store: &StoreConfig,
    ) -> Result<Self, ConfigError> {
        let stripe_count = stripe_count.or(store.stripe_count);
        if let Some(count) = stripe_count {
            validate_stripe_count(count)?;
        }
        Ok(Self {
            metadata_db: metadata_db
                .or(store.metadata_db)
                .unwrap_or(DEFAULT_METADATA_DB),
            durability: durability
                .or(store.durability)
                .unwrap_or(DEFAULT_DURABILITY),
            inline_metadata_size: inline_metadata_size.or(store.inline_metadata_size),
            verify_on_read: store.verify_on_read.unwrap_or(DEFAULT_VERIFY_ON_READ),
            hasher: store.hash.hasher()?,
            stripe_count,
        })
    }

    /// The header a store created with these options gets.
    pub fn header_spec(&self) -> HeaderSpec {
        HeaderSpec::from(self.hasher)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store_config(toml: &str) -> StoreConfig {
        crate::config::parse(toml, std::path::Path::new("test.toml"))
            .expect("test config must parse")
            .store
    }

    #[test]
    fn defaults_apply_when_nothing_is_set() {
        let opts = StoreOptions::resolve(None, None, None, None, &StoreConfig::default()).unwrap();
        assert_eq!(opts.metadata_db, StorageEngine::Fjall);
        assert_eq!(opts.durability, Durability::Fsync);
        assert_eq!(opts.inline_metadata_size, None);
        assert!(!opts.verify_on_read);
        assert_eq!(opts.hasher, Hasher::Blake3W32);
        // None, not the number: the default is applied by the store, so the
        // constant stays the single source of truth.
        assert_eq!(opts.stripe_count, None);
    }

    #[test]
    fn config_beats_the_defaults() {
        let store = store_config(
            "[store]\ndurability = \"buffer\"\nmetadata_db = \"fjall\"\n\
             inline_metadata_size = 512\nverify_on_read = true\nstripe_count = 256\n\n\
             [store.hash]\nwidth = 16\n",
        );
        let opts = StoreOptions::resolve(None, None, None, None, &store).unwrap();
        assert_eq!(opts.metadata_db, StorageEngine::Fjall);
        assert_eq!(opts.durability, Durability::Buffer);
        assert_eq!(opts.inline_metadata_size, Some(512));
        assert!(opts.verify_on_read);
        assert_eq!(opts.hasher, Hasher::Blake3W16);
        assert_eq!(opts.header_spec().hash_width, 16);
        assert_eq!(opts.stripe_count, Some(256));
    }

    #[test]
    fn cli_beats_the_config() {
        let store = store_config(
            "[store]\ndurability = \"buffer\"\nmetadata_db = \"fjall\"\n\
             inline_metadata_size = 512\nstripe_count = 256\n",
        );
        let opts = StoreOptions::resolve(
            Some(StorageEngine::Fjall),
            Some(Durability::Fdatasync),
            Some(64),
            Some(2048),
            &store,
        )
        .unwrap();
        assert_eq!(opts.metadata_db, StorageEngine::Fjall);
        assert_eq!(opts.durability, Durability::Fdatasync);
        assert_eq!(opts.inline_metadata_size, Some(64));
        assert_eq!(opts.stripe_count, Some(2048), "the flag must beat the file");
    }

    /// The full precedence ladder for the stripe count in one place: flag over
    /// file over default.
    #[test]
    fn stripe_count_precedence_is_flag_then_file_then_default() {
        let none = store_config("[store]\n");
        let file = store_config("[store]\nstripe_count = 256\n");

        // Neither: left as None, so the store applies DEFAULT_STRIPE_COUNT.
        assert_eq!(
            StoreOptions::resolve(None, None, None, None, &none)
                .unwrap()
                .stripe_count,
            None
        );
        // File only.
        assert_eq!(
            StoreOptions::resolve(None, None, None, None, &file)
                .unwrap()
                .stripe_count,
            Some(256)
        );
        // Flag only.
        assert_eq!(
            StoreOptions::resolve(None, None, None, Some(2048), &none)
                .unwrap()
                .stripe_count,
            Some(2048)
        );
        // Both: the flag.
        assert_eq!(
            StoreOptions::resolve(None, None, None, Some(2048), &file)
                .unwrap()
                .stripe_count,
            Some(2048)
        );
    }

    /// A bad stripe count from the CLI must be refused too. The parse-time
    /// check only ever sees the file, so without this the flag is the one way
    /// an unusable count reaches a store.
    #[test]
    fn a_bad_stripe_count_flag_is_refused() {
        let store = StoreConfig::default();
        let err = StoreOptions::resolve(None, None, None, Some(0), &store).unwrap_err();
        assert!(err.to_string().contains('0'), "{err}");

        let err = StoreOptions::resolve(
            None,
            None,
            None,
            Some(crate::config::MAX_STRIPE_COUNT + 1),
            &store,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("stripe count"),
            "message must name the setting: {err}"
        );
    }

    #[test]
    fn a_bad_hash_section_is_refused() {
        let store = StoreConfig {
            hash: crate::config::HashConfig {
                algo: None,
                width: Some(24),
            },
            ..StoreConfig::default()
        };
        let err = StoreOptions::resolve(None, None, None, None, &store).unwrap_err();
        assert!(err.to_string().contains("24"), "{err}");
    }
}
