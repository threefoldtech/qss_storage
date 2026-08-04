use std::str::FromStr;

/// Which metadata database backend a store uses.
///
/// Deserialized through [`FromStr`] (`try_from = "String"`) so the config file
/// spelling is exactly the CLI flag spelling: `fjall`.
///
/// A single variant since ADR 0007 removed the non-transactional backend;
/// the enum survives as the config/CLI surface, and [`FromStr`] rejects the
/// removed value with the migration path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(try_from = "String")]
pub enum StorageEngine {
    // fjall with transactions support
    Fjall,
}

impl FromStr for StorageEngine {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "fjall" => Ok(StorageEngine::Fjall),
            "fjall_notx" => Err(
                "the fjall_notx backend was removed (ADR 0007); use fjall with \
                 durability = \"buffer\" for the fast tier"
                    .to_string(),
            ),
            _ => Err(format!("unknown storage engine: {s} (expected fjall)")),
        }
    }
}

impl TryFrom<String> for StorageEngine {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl std::fmt::Display for StorageEngine {
    /// Writes the spelling [`FromStr`] accepts, so a value read from a config
    /// file round-trips through a log line unchanged.
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let name = match self {
            StorageEngine::Fjall => "fjall",
        };
        f.write_str(name)
    }
}
