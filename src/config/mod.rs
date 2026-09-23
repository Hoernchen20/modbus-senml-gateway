pub mod schema;
pub mod validate;

pub use schema::Config;
pub use validate::ValidationError;

use std::path::Path;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file '{path}': {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse config file '{path}': {source}")]
    Parse {
        path: String,
        #[source]
        source: toml::de::Error,
    },

    #[error(transparent)]
    Validation(#[from] ValidationError),
}

pub fn load(path: impl AsRef<Path>) -> Result<Config, ConfigError> {
    let path_str = path.as_ref().display().to_string();

    let contents = std::fs::read_to_string(path.as_ref()).map_err(|e| ConfigError::Read {
        path: path_str.clone(),
        source: e,
    })?;

    let config: Config = toml::from_str(&contents).map_err(|e| ConfigError::Parse {
        path: path_str.clone(),
        source: e,
    })?;

    validate::validate(&config)?;

    Ok(config)
}
