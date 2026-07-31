//! Store settings resolved from CLI flags, the config file, and the built-in
//! defaults -- in that order of precedence.
//!
//! Every s3cas subcommand opens a store, and all of them have to agree on how:
//! a tool that opens the store with a different backend or a different inline
//! threshold than the server wrote it with reads the wrong thing. So the
//! resolution lives here once, and `server`, `inspect`, `retrieve` and `check`
//! all call it.

use cas_storage::config::{
    ConfigError, DEFAULT_DURABILITY, DEFAULT_METADATA_DB, DEFAULT_VERIFY_ON_READ, StoreConfig,
};
use cas_storage::{Durability, Hasher, HeaderSpec, StorageEngine};

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
    /// [`ConfigError`] if `store.hash` does not name a hash this build has.
    pub fn resolve(
        metadata_db: Option<StorageEngine>,
        durability: Option<Durability>,
        inline_metadata_size: Option<usize>,
        store: &StoreConfig,
    ) -> Result<Self, ConfigError> {
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
        cas_storage::config::parse(toml, std::path::Path::new("test.toml"))
            .expect("test config must parse")
            .store
    }

    #[test]
    fn defaults_apply_when_nothing_is_set() {
        let opts = StoreOptions::resolve(None, None, None, &StoreConfig::default()).unwrap();
        assert_eq!(opts.metadata_db, StorageEngine::Fjall);
        assert_eq!(opts.durability, Durability::Fsync);
        assert_eq!(opts.inline_metadata_size, None);
        assert!(!opts.verify_on_read);
        assert_eq!(opts.hasher, Hasher::Blake3W32);
    }

    #[test]
    fn config_beats_the_defaults() {
        let store = store_config(
            "[store]\ndurability = \"buffer\"\nmetadata_db = \"fjall\"\n\
             inline_metadata_size = 512\nverify_on_read = true\n\n\
             [store.hash]\nwidth = 16\n",
        );
        let opts = StoreOptions::resolve(None, None, None, &store).unwrap();
        assert_eq!(opts.metadata_db, StorageEngine::Fjall);
        assert_eq!(opts.durability, Durability::Buffer);
        assert_eq!(opts.inline_metadata_size, Some(512));
        assert!(opts.verify_on_read);
        assert_eq!(opts.hasher, Hasher::Blake3W16);
        assert_eq!(opts.header_spec().hash_width, 16);
    }

    #[test]
    fn cli_beats_the_config() {
        let store = store_config(
            "[store]\ndurability = \"buffer\"\nmetadata_db = \"fjall\"\n\
             inline_metadata_size = 512\n",
        );
        let opts = StoreOptions::resolve(
            Some(StorageEngine::Fjall),
            Some(Durability::Fdatasync),
            Some(64),
            &store,
        )
        .unwrap();
        assert_eq!(opts.metadata_db, StorageEngine::Fjall);
        assert_eq!(opts.durability, Durability::Fdatasync);
        assert_eq!(opts.inline_metadata_size, Some(64));
    }

    #[test]
    fn a_bad_hash_section_is_refused() {
        let store = StoreConfig {
            hash: cas_storage::config::HashConfig {
                algo: None,
                width: Some(24),
            },
            ..StoreConfig::default()
        };
        let err = StoreOptions::resolve(None, None, None, &store).unwrap_err();
        assert!(err.to_string().contains("24"), "{err}");
    }
}
